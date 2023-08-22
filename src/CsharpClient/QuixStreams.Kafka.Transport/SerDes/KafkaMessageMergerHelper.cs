using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using QuixStreams.Kafka.Transport.SerDes.Legacy;

namespace QuixStreams.Kafka.Transport.SerDes
{
    /// <summary>
    /// Merges messages split by <see cref="KafkaMessageSplitter" />
    /// </summary>
    internal class KafkaMessageMergerHelper
    {
        private readonly KafkaMessageBuffer buffer;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaMessageMergerHelper" />
        /// </summary>
        /// <param name="buffer">The buffer to use to store the message parts</param>
        /// <param name="logger">The logger to use</param>
        internal KafkaMessageMergerHelper(KafkaMessageBuffer buffer, ILogger logger)
        {
            this.buffer = buffer;
            this.logger = logger ?? NullLogger.Instance;
            this.buffer.OnMessagePurged += (ea) =>
            {
                this.OnMessageSegmentsPurged?.Invoke(ea.BufferId);
            };
        }

        /// <inheritdoc />
        public MessageMergeResult TryMerge(KafkaMessage messageSegment, out MergerBufferId mergerBufferId, out KafkaMessage message)
        {
            var key = messageSegment.Key == null ? string.Empty : Constants.Utf8NoBOMEncoding.GetString(messageSegment.Key);
            mergerBufferId = default(MergerBufferId);
            var messageIdBytes = messageSegment.Headers
                ?.FirstOrDefault(y => y.Key == Constants.KafkaMessageHeaderSplitMessageId)?.Value;
            if (messageIdBytes == null)
            {
                if (this.TryConvertLegacySplitMessage(messageSegment, out var newMessage))
                {
                    return this.TryMerge(newMessage, out mergerBufferId, out message);
                }
                
                // not something we know how to merge
                message = messageSegment;
                return MessageMergeResult.Unmerged;
            }

            var messageCountBytes = messageSegment.Headers?.FirstOrDefault(y =>
                y.Key == Constants.KafkaMessageHeaderSplitMessageCount)?.Value;
            if (messageCountBytes == null)
            {
                // not something we know how to merge
                message = messageSegment;
                return MessageMergeResult.Unmerged;
            }

            var messageIndexBytes = messageSegment.Headers?.FirstOrDefault(y =>
                y.Key == Constants.KafkaMessageHeaderSplitMessageIndex)?.Value;
            if (messageIndexBytes == null)
            {
                // not something we know how to merge
                message = messageSegment;
                return MessageMergeResult.Unmerged;
            }

            var messageId = Convert.ToBase64String(messageIdBytes); // Whether it is guid or not, doesn't matter, uniqueness matters
            var messageIndex = BitConverter.ToInt32(messageIndexBytes, 0);
            var messageCount = BitConverter.ToInt32(messageCountBytes, 0);

            var bufferId = new MergerBufferId(key, messageId);
            if (messageIndex != 0)
            {
                if (!this.buffer.Exists(bufferId))
                {
                    message = null;
                    return MessageMergeResult.Discarded;
                }
            }
            this.buffer.Add(bufferId, messageIndex, messageCount, messageSegment);
            mergerBufferId = new MergerBufferId(key, messageId);
            var lastMsgIndex = messageCount - 1; // 0 based, so with 50 segment, 49 would be last
            if (messageIndex != lastMsgIndex)
            {
                // we do not have all parts yet if assuming proper message order
                message = null;
                return MessageMergeResult.MergePending;
            }

            // last message! Lets check if we have all parts;

            message = this.AssembleMessage(bufferId, ref mergerBufferId);
            if (message == null) return MessageMergeResult.Discarded;
            return MessageMergeResult.Merged;
        }
        
        

        /// <summary>
        /// Attempts to convert the kafka message split details if it was done using legacy split methodology
        /// </summary>
        /// <param name="message">The message to convert</param>
        /// <param name="convertedMessage">The new message if method returns true</param>
        /// <returns>True if there message is converted, false otherwise</returns>
        internal bool TryConvertLegacySplitMessage(KafkaMessage message, out KafkaMessage convertedMessage)
        {
            convertedMessage = null;
            if (!LegacyByteSplitter.TryGetSplitDetails(message.Value, out var msgId, out var msgIndex, out var lastMsgIndex, out var msgData))
            {
                return false;
            }

            int messageCount = lastMsgIndex + 1; // 0 based index, so last index for 50 count would be 49

            var idAsBytes = BitConverter.GetBytes(msgId); // new ones use guid, but reader side shouldn't actually care what it is
            var headerDict = KafkaMessageSplitter.CreateSegmentDictionary(msgIndex, BitConverter.GetBytes(messageCount), idAsBytes);
            var messageHeaders = message.Headers?.ToList() ?? new List<KafkaHeader>();
            foreach (var header in headerDict)
            {
                messageHeaders.Add(new KafkaHeader(header.Key, header.Value));
            }

            convertedMessage = new KafkaMessage(message.Key, msgData, messageHeaders.ToArray(), message.Timestamp, message.TopicPartitionOffset);

            return true;
        }

        /// <inheritdoc />
        public void Purge(MergerBufferId bufferId)
        {
            this.buffer.Remove(bufferId, out _, out var count);
        }

        /// <inheritdoc />
        public event Action<MergerBufferId> OnMessageSegmentsPurged;

        private MergedKafkaMessage AssembleMessage(MergerBufferId bufferId, ref MergerBufferId messageGroupId)
        {
            var completedBuffer = this.buffer.Remove(bufferId, out var bufferSize, out var segmentCount);
            if (segmentCount == 0)
            {
                // message never was or no longer in buffer !
                logger.LogDebug("Received last segment for {0}, but rest of the segments can no longer be found.", messageGroupId);
                messageGroupId = default;
                return null;
            }
            
            // Verify all segments are available quickly
            for (var msgIndex = 0; msgIndex < completedBuffer.Count; msgIndex++)
            {
                var message = completedBuffer[msgIndex];
                var indexBytes = message?.Headers?.FirstOrDefault(y=>
                    y.Key == Constants.KafkaMessageHeaderSplitMessageIndex)
                    ?.Value;
                if (indexBytes == null)
                {
                    logger.LogDebug("Received last segment for {0}, but some of the segments can no longer be found.",
                        messageGroupId);
                    return null;
                }

                var actualMsgIndex = BitConverter.ToInt32(indexBytes, 0);
                if (actualMsgIndex != msgIndex)
                {
                    logger.LogDebug("Received last segment for {0}, but some of the segments can no longer be found.", messageGroupId);
                    return null;
                }
            }

            // Merge data into buffer
            var msgBuffer = new byte[bufferSize];
            var destinationIndex = 0;
            var sourceMessages = new KafkaMessage[completedBuffer.Count];
            for (var index = 0; index < completedBuffer.Count; index++)
            {
                var messageSegment = completedBuffer[index];
                if (messageSegment == null)
                {
                    logger.LogDebug("Received last segment for {0}, but some segments can no longer be found.", messageGroupId);
                    messageGroupId = default;
                    return null;
                }
                Array.Copy(messageSegment.Value, 0, msgBuffer, destinationIndex, messageSegment.Value.Length);
                destinationIndex += messageSegment.Value.Length;
                sourceMessages[index] = new KafkaMessage(messageSegment.Key, Array.Empty<byte>(),
                    messageSegment.Headers, messageSegment.Timestamp,
                    messageSegment.TopicPartitionOffset);  // In order to not keep duplicate in memory
            }

            var firstMessage = sourceMessages[0];
            List<KafkaHeader> headers = null;
            if (firstMessage.Headers != null)
            {
                headers = firstMessage.Headers.Where(y => y.Key != Constants.KafkaMessageHeaderSplitMessageIndex)
                    .ToList();
            }
            
            return new MergedKafkaMessage(sourceMessages, firstMessage.Key, msgBuffer, headers?.ToArray(), firstMessage.TopicPartitionOffset);
        }
    }
    
    public struct MergerBufferId : IComparable<MergerBufferId>, IEquatable<MergerBufferId>
    {
        public string Key { get; }
        public string MessageId { get; }

        public MergerBufferId(string messageKey, string messageId)
        {
            this.Key = messageKey ?? String.Empty;
            this.MessageId = messageId ?? String.Empty;
        }

        public MergerBufferId(KafkaMessage message)
        {
            this.Key = message.Key == null ? string.Empty : Constants.Utf8NoBOMEncoding.GetString(message.Key);
            var messageIdBytes = message.Headers?.FirstOrDefault(y =>
                y.Key == Constants.KafkaMessageHeaderSplitMessageId)?.Value;
            if (messageIdBytes == null)
            {
                this.MessageId = Guid.NewGuid().ToString("N");
                return;
            }
            this.MessageId = Constants.Utf8NoBOMEncoding.GetString(messageIdBytes);
        }

        public int CompareTo(MergerBufferId other)
        {
            var keyComparison = string.Compare(Key, other.Key, StringComparison.Ordinal);
            if (keyComparison != 0) return keyComparison;
            return string.Compare(MessageId, other.MessageId, StringComparison.Ordinal);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Key != null ? Key.GetHashCode() : 0) * 397) ^ (MessageId != null ? MessageId.GetHashCode() : 0);
            }
        }

        public bool Equals(MergerBufferId other)
        {
            return Key == other.Key && MessageId == other.MessageId;
        }

        public override bool Equals(object obj)
        {
            return obj is MergerBufferId other && Equals(other);
        }

        public override string ToString()
        {
            return $"{Key}-{MessageId}";
        }
    }

    internal enum MessageMergeResult
    {
        /// <summary>
        /// The message is not merged, because it is not a segment of a split message
        /// </summary>
        Unmerged,
        
        /// <summary>
        /// The message is discarded because previous segments are missing and will not be available
        /// </summary>
        Discarded,
        
        /// <summary>
        /// The message is a split message and not all segments are available yet
        /// </summary>
        MergePending,
        
        /// <summary>
        /// The message is a split message and all segments are merged into one
        /// </summary>
        Merged
    }
}