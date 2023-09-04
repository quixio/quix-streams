using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace QuixStreams.Kafka.Transport.SerDes
{
    /// <summary>
    /// Buffer capable of handling a single message Id.
    /// </summary>
    public class KafkaMessageBuffer
    {
        private class BufferedValue
        {
            public BufferedValue(MergerBufferId bufferId, int msgCount)
            {
                this.BufferId = bufferId;
                this.MessageBuffer = new KafkaMessage[msgCount];
            }

            public readonly MergerBufferId BufferId; // The ID of the message in the buffer
            public DateTimeOffset LastUpdate = DateTimeOffset.Now;
            public readonly KafkaMessage[] MessageBuffer;
            public int MessageLength = 0;
        }

        private readonly TimeSpan timeToLive;
        private readonly int bufferPerMessageGroupKey;
        private readonly object valueBufferLock = new object();
        private DateTimeOffset lastTtlCheck = DateTimeOffset.Now; 

        private readonly Dictionary<MergerBufferId, BufferedValue[]> msgGroupBuffers = new Dictionary<MergerBufferId, BufferedValue[]>();
        private readonly ILogger logger;
        
        /// <summary>
        /// Raised when members of the specified message have been purged. Reason could be timout or similar.
        /// </summary>
        public event Action<MessagePurgedEventArgs> OnMessagePurged;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaMessageBuffer"/>
        /// </summary>
        /// <param name="bufferPerMessageGroupKey">The number of different buffered message ids a group can have concurrently. Higher number might help with a producer that is interweaving multiple split message</param>
        public KafkaMessageBuffer(int bufferPerMessageGroupKey = 50) : this(TimeSpan.FromSeconds(60), bufferPerMessageGroupKey)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaMessageBuffer"/> 
        /// </summary>
        /// <param name="timeToLive">Time to live for messages that do not properly merge for various reasons. This time is after last message appended to buffer for the message Group Key and message id</param>
        /// <param name="bufferPerMessageGroupKey">The number of different buffered message ids a group can have concurrently. Higher number might help with a producer that is interweaving multiple split message</param>
        public KafkaMessageBuffer(TimeSpan timeToLive, int bufferPerMessageGroupKey = 50)
        {
            if (bufferPerMessageGroupKey < 1) throw new ArgumentOutOfRangeException(nameof(bufferPerMessageGroupKey), "Value must be at least 1");
            this.timeToLive = timeToLive;
            this.bufferPerMessageGroupKey = bufferPerMessageGroupKey;
            this.logger = QuixStreams.Logging.CreateLogger(typeof(KafkaMessageBuffer));
        }

        /// <summary>
        /// Adds the message segment to the buffer
        /// </summary>
        /// <param name="bufferId">An unique buffer id</param>
        /// <param name="messageIndex">The message index of this segment</param>
        /// <param name="messageCount">The total number of message segments</param>
        /// <param name="messageSegment">The message segment</param>
        public void Add(MergerBufferId bufferId, int messageIndex, int messageCount, KafkaMessage messageSegment)
        {
            lock (this.valueBufferLock)
            {
                var msgBuffer = this.GetOrCreateMessageBuffer(bufferId, messageCount);
                
                if (msgBuffer.MessageBuffer[messageIndex] != null)
                {
                    // We have this segment already ?
                    this.logger.LogTrace("Duplicate message, group key: {0}, msg id: {1}, msg index: {2}", bufferId.Key, bufferId.MessageId, messageIndex);
                }
                else
                {
                    msgBuffer.MessageLength += messageSegment.Value.Length;
                    msgBuffer.MessageBuffer[messageIndex] = messageSegment;
                    msgBuffer.LastUpdate = DateTimeOffset.Now;
                }

                PerformTtlCheck();
            }
        }

        /// <summary>
        /// Retrieves the list of buffer ids currently handled by the buffer
        /// </summary>
        /// <returns>The managed buffer ids</returns>
        public IList<MergerBufferId> GetBufferIds()
        {
            lock (this.valueBufferLock)
            {
                return this.msgGroupBuffers.Keys.ToList();
            }
        }

        /// <summary>
        /// Returns whether the specified message group key and id combination exists
        /// </summary>
        /// <param name="bufferId">An unique buffer id</param>
        /// <returns>True if the specified message group key and id combination exists, otherwise false</returns>
        public bool Exists(MergerBufferId bufferId)
        {
            lock (this.valueBufferLock)
            {
                // TODO the second part fo the check might be unnecessary
                return this.msgGroupBuffers.TryGetValue(bufferId, out var groupBuffers) && groupBuffers.Any(x => x != null && x.BufferId.Equals(bufferId));
            }
        }

        private BufferedValue GetOrCreateMessageBuffer(MergerBufferId bufferId, int totalMessageCount)
        {
            if (!this.msgGroupBuffers.TryGetValue(bufferId, out var groupBuffers))
            {
                groupBuffers = new BufferedValue[this.bufferPerMessageGroupKey];
                this.msgGroupBuffers[bufferId] = groupBuffers;
            }

            var msgBuffer = groupBuffers.FirstOrDefault(x => x != null && x.BufferId.Equals(bufferId));
            if (msgBuffer == null)
            {
                // check if there is a free slot
                var indexToUse = Array.IndexOf(groupBuffers, null);
                if (indexToUse == -1)
                {
                    // time to kick out one
                    var kickOut = groupBuffers.OrderBy(x => x.LastUpdate).First();
                    indexToUse = Array.IndexOf(groupBuffers, kickOut);
                    this.logger.LogWarning("Concurrent split message track count reached, dropping oldest msg with segments. Group key: {0}, msg id: {1}", bufferId, kickOut.BufferId);
                    this.OnMessagePurged?.Invoke(new MessagePurgedEventArgs(bufferId));
                }

                msgBuffer = new BufferedValue(bufferId, totalMessageCount);
                groupBuffers[indexToUse] = msgBuffer;
            }

            return msgBuffer;
        }
        
        private BufferedValue RemoveMessageBuffer(MergerBufferId bufferId)
        {
            if (!this.msgGroupBuffers.TryGetValue(bufferId, out var groupBuffers))
            {
                return null;
            }

            var msgBuffer = groupBuffers.FirstOrDefault(x => x != null && x.BufferId.Equals(bufferId));
            if (msgBuffer == null)
            {
                return null;
            }

            var indexToFree = Array.IndexOf(groupBuffers, msgBuffer);
            groupBuffers[indexToFree] = null; // free it up
            
            // check if the msgGroup is empty, if so, remove
            if (groupBuffers.All(x => x == null))
            {
                this.msgGroupBuffers.Remove(bufferId);
            }
            
            return msgBuffer;
        }

        /// <summary>
        /// Purges the segments from the buffer with the given bufferId
        /// </summary>
        /// <param name="bufferId">The buffer id to purge</param>
        /// <returns></returns>
        internal bool Purge(MergerBufferId bufferId)
        {
            lock (this.valueBufferLock)
            {
                if (this.msgGroupBuffers.Remove(bufferId))
                {
                    this.logger.LogTrace("Message purged: {0}, msg id: {1}.", bufferId.Key, bufferId.MessageId);
                    this.OnMessagePurged?.Invoke(new MessagePurgedEventArgs(bufferId));
                    return true;
                }
            }

            return false;
        }

        private void PerformTtlCheck()
        {
            if (this.lastTtlCheck.AddTicks(timeToLive.Ticks / 10) > DateTimeOffset.Now) return; // check max N times per TTL duration to avoid spamming
            this.lastTtlCheck = DateTimeOffset.Now;
            var cutoff = DateTimeOffset.Now - timeToLive;
            var purged = new List<MergerBufferId>();
            foreach (var msgGroupBuffer in msgGroupBuffers)
            {
                for (var index = 0; index < msgGroupBuffer.Value.Length; index++)
                {
                    var msgSegment = msgGroupBuffer.Value[index];
                    if (msgSegment == null) continue;
                    if (msgSegment.LastUpdate > cutoff) continue; // not old enough
                    purged.Add(msgGroupBuffer.Key);
                    break;
                }
            }

            foreach (var mergerBufferId in purged)
            {
                this.msgGroupBuffers.Remove(mergerBufferId);
                this.logger.LogWarning("Message segment expired, only a part of the message was received within allowed time. Group key: {0}, msg id: {1}.", mergerBufferId.Key, mergerBufferId.MessageId);
                this.OnMessagePurged?.Invoke(new MessagePurgedEventArgs(mergerBufferId));
            }
        }

        /// <summary>
        /// Removes the message segments from the buffer and returns them in the order according to their message index
        /// </summary>
        /// <param name="bufferId">A unique message identifier</param>
        /// <param name="segmentLengths">
        /// It is set to 0 if message is not in buffer, else to the data length of the segments for the message id
        /// </param>
        /// <param name="messageCount">It is set to 0 if message is not in buffer, else to total number of messages for the message id</param>
        /// <returns>The messages segments in order. Returns zero length if requested message segments are unavailable</returns>
        public IReadOnlyList<KafkaMessage> Remove(MergerBufferId bufferId, out int segmentLengths, out int messageCount)
        {
            lock (this.valueBufferLock)
            {
                var buffer = RemoveMessageBuffer(bufferId);
                PerformTtlCheck();
                if (buffer == null)
                {
                    segmentLengths = 0;
                    messageCount = 0;
                    return null;
                }

                segmentLengths = buffer.MessageLength;
                messageCount = (byte) buffer.MessageBuffer.Length;

                var val = buffer.MessageBuffer;
                return val;
            }
        }

        /// <summary>
        /// Message Purged event arguments
        /// </summary>
        public class MessagePurgedEventArgs
        {
            /// <summary>
            /// Message group key
            /// </summary>
            public readonly MergerBufferId BufferId;

            /// <summary>
            /// Initializes a new instance of <see cref="MessagePurgedEventArgs"/>
            /// </summary>
            /// <param name="bufferId">Message group key</param>
            public MessagePurgedEventArgs(MergerBufferId bufferId)
            {
                this.BufferId = bufferId;
            }
        }
    }
}