using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams.Kafka.Transport.SerDes.Legacy;

namespace QuixStreams.Kafka.Transport.SerDes
{
    /// <summary>
    /// 
    /// </summary>
    public class KafkaMessageMerger
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<KafkaMessageMerger>();

        private long bufferCounter = 0;
        private readonly ConcurrentDictionary<MergerBufferId, PendingKafkaMessage> pendingMessages = new ConcurrentDictionary<MergerBufferId, PendingKafkaMessage>(); // Messages that are queued up
        private readonly SortedDictionary<long, MergerBufferId> packageOrder = new SortedDictionary<long, MergerBufferId>(); // the order the packages should be raised

        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1,1); // see https://blog.cdemi.io/async-waiting-inside-c-sharp-locks/
        private readonly KafkaMessageMergerHelper helper;
        /// <summary>
        /// Initializes a new instance of <see cref="KafkaMessageMerger"/>
        /// </summary>
        public KafkaMessageMerger(KafkaMessageBuffer messageBuffer)
        {
            this.helper = new KafkaMessageMergerHelper(messageBuffer, this.logger);
            this.helper.OnMessageSegmentsPurged += (bufferId) =>
            {
                if (this.RemoveFromBuffer(bufferId))
                {
                    RaiseNextPackageIfReady().GetAwaiter().GetResult();
                }
            };
        }

        /// <summary>
        /// The callback that is used when new message (merged or not) is available
        /// </summary>
        public Func<KafkaMessage, Task> OnMessageAvailable { get; set; }

        /// <summary>
        /// Merges the provided kafka message with previously provided messages, if needed. Merge results are raised via <see cref="OnMessageAvailable"/>,
        /// as there is no direct relation to message merged and message raised due to ordering and other reasons.
        /// </summary>
        /// <param name="kafkaMessage">The kafka message to merge</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        public Task Merge(KafkaMessage kafkaMessage)
        {
            if (this.helper.TryConvertLegacySplitMessage(kafkaMessage, out var convertedMessage)) kafkaMessage = convertedMessage;
            
            var mergeResult = this.helper.TryMerge(kafkaMessage, out var bufferId, out var mergedMessage);

            if (mergeResult == MessageMergeResult.Discarded) return Task.CompletedTask;

            if (mergeResult == MessageMergeResult.MergePending)
            {
                if (bufferId.Equals(default))
                {
                    // Means that the message is invalid, due to buffering // missing data constraints
                    // TODO: possibly legacy stuff and no longer necessary check
                    return Task.CompletedTask;
                } 
                TryAddToBuffer(ref bufferId, new PendingKafkaMessage(kafkaMessage, false));
                return Task.CompletedTask;
            }

            // By this point the merged message can't be null, meaning that it was either a standalone message
            // that never had ny merging to do, or it was a final missing segment which completed a merge.
            KafkaMessage messageToRaise = null;
            if (mergeResult == MessageMergeResult.Unmerged)
            {
                // null buffer id means that this is not a merged message
                // lets use original message completely
                messageToRaise = kafkaMessage;
            }
            else
            {
                // buffer id means that this is a merged message
                messageToRaise = mergedMessage;
            }
            
            Debug.Assert(messageToRaise != null);
            
            // check if empty. We're not worried about threading here, because this method is designed to be invoked via single thread
            // and any external thread will only ever reduce it, not increment. (see OnMessageSegmentsPurged)
            if (this.bufferCounter == 0)
            {
                RemoveFromBuffer(bufferId);
                return this.OnMessageAvailable?.Invoke(messageToRaise) ?? Task.CompletedTask;
            }
            
            // Not empty, check if this is next in line
            if (mergeResult == MessageMergeResult.Unmerged)
            {
                // can't be next in line. No buffer id tells us it isn't a buffered value. Given there are other values in the buffer already, this can't possibly be the next.
                TryAddToBuffer(ref bufferId, new PendingKafkaMessage(messageToRaise, true));
            }
            else
            {
                // Could be next, but we don't know yet. Let's update in the buffer
                this.pendingMessages[bufferId] = new PendingKafkaMessage(messageToRaise, true);
            }

            return RaiseNextPackageIfReady();
        }
        
        private async Task RaiseNextPackageIfReady()
        {
            // The logic here has to be locked, because it touches multiple objects based on condition of other ones
            await semaphoreSlim.WaitAsync();
            try
            {
                // lets figure out what is the next
                foreach (var pair in packageOrder.ToList()) // to avoid issues with removing from it
                {
                    var nextBufferId = pair.Value;
                    if (!pendingMessages.TryGetValue(nextBufferId, out var nextMessage))
                    {
                        // It may have gotten removed due to buffer purge or other reasons
                        continue;
                    }
                    
                    if (nextMessage == null)
                    {
                        // the next transportPackage in line is not yet ready.
                        return;
                    }

                    if (!nextMessage.Ready)
                    {
                        // The message can't be released yet
                        return;
                    }

                    await (this.OnMessageAvailable?.Invoke(nextMessage.Message) ?? Task.CompletedTask); 
                    RemoveFromBuffer(nextBufferId);
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private bool TryAddToBuffer(ref MergerBufferId bufferId, PendingKafkaMessage message)
        {
            long order; 
            if (bufferId.Equals(default)) 
            {
                order = Interlocked.Increment(ref bufferCounter);
                bufferId = new MergerBufferId(message.Message); // not worried about buffer removal here, because only case this should happen if it never was buffered
                if (!pendingMessages.TryAdd(bufferId, message)) return false; // not the end of the world to not reduce bufferOrder even if failing... however failure here is "a probably never"
            }
            else
            {
                if (!pendingMessages.TryAdd(bufferId, message)) return false;
                 order = Interlocked.Increment(ref bufferCounter);
            }

            packageOrder[order] = bufferId;
            return true;
        }
        
        private bool RemoveFromBuffer(MergerBufferId bufferId)
        {
            if (bufferId.Equals(default)) return false;
            if (!pendingMessages.TryRemove(bufferId, out _)) return false;
            this.helper.Purge(bufferId);
            return true;
        }

        public void HandleRevoked(RevokedEventArgs args)
        {
            if (args.Revoked.Count == 0) return;
            var merges = this.pendingMessages.ToArray();
            if (merges.Length == 0) return; // there is nothing to do

            var partitionsAffected = args.Revoked.Select(y => y.TopicPartition).ToList();

            foreach (var keyValuePair in merges)
            {
                if (!partitionsAffected.Contains(keyValuePair.Value.Message.TopicPartitionOffset.TopicPartition)) continue;
                this.RemoveFromBuffer(keyValuePair.Key);
            }
            RaiseNextPackageIfReady().GetAwaiter().GetResult();
        }

        private class PendingKafkaMessage
        {
            /// <summary>
            /// The Kafka message that is pending
            /// </summary>
            public readonly KafkaMessage Message;
            
            /// <summary>
            /// Whether the message is ready to be released
            /// </summary>
            public readonly bool Ready;

            public PendingKafkaMessage(KafkaMessage message, bool ready)
            {
                this.Message = message;
                this.Ready = ready;
            }
        }
    }
}