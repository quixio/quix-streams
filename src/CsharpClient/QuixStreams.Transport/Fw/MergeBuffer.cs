using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams;

namespace QuixStreams.Transport.Fw
{
    /// <summary>
    /// Buffer capable of handling a single message Id.
    /// </summary>
    public class MergeBuffer
    {
        private class BufferedValue
        {
            public BufferedValue(int msgIndex, int lastMsgIndex)
            {
                this.MessageId = msgIndex;
                this.ValueBuffer = new byte[lastMsgIndex + 1][];
            }

            public readonly int MessageId; // The ID of the message in the buffer
            public DateTimeOffset LastUpdate = DateTimeOffset.Now;
            public readonly byte[][] ValueBuffer;
            public int MessageLength = 0;
        }

        private readonly TimeSpan timeToLive;
        private readonly int bufferPerMessageGroupKey;
        private readonly object valueBufferLock = new object();
        private DateTimeOffset lastTtlCheck = DateTimeOffset.Now; 

        private readonly Dictionary<string, BufferedValue[]> msgGroupBuffers = new Dictionary<string, BufferedValue[]>();
        private readonly ILogger logger;
        
        /// <summary>
        /// Raised when members of the specified message have been purged. Reason could be timout or similar.
        /// </summary>
        public event Action<MessagePurgedEventArgs> OnMessagePurged;

        /// <summary>
        /// Initializes a new instance of <see cref="MergeBuffer"/>
        /// </summary>
        /// <param name="bufferPerMessageGroupKey">The number of different buffered message ids a group can have concurrently. Higher number might help with a producer that is interweaving multiple split message</param>
        public MergeBuffer(int bufferPerMessageGroupKey = 50) : this(TimeSpan.FromSeconds(60), bufferPerMessageGroupKey)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="MergeBuffer"/> 
        /// </summary>
        /// <param name="timeToLive">Time to live for messages that do not properly merge for various reasons. This time is after last message appended to buffer for the message Group Key and message id</param>
        /// <param name="bufferPerMessageGroupKey">The number of different buffered message ids a group can have concurrently. Higher number might help with a producer that is interweaving multiple split message</param>
        public MergeBuffer(TimeSpan timeToLive, int bufferPerMessageGroupKey = 50)
        {
            if (bufferPerMessageGroupKey < 1) throw new ArgumentOutOfRangeException(nameof(bufferPerMessageGroupKey), "Value must be at least 1");
            this.timeToLive = timeToLive;
            this.bufferPerMessageGroupKey = bufferPerMessageGroupKey;
            this.logger = Logging.CreateLogger(typeof(MergeBuffer));
        }

        /// <summary>
        /// Adds the message segment to the buffer
        /// </summary>
        /// <param name="msgGroupKey">An unique key, which further identifies, which group the message belongs to</param>
        /// <param name="messageId">The message id the segment belongs to</param>
        /// <param name="messageIndex">The message index of this segment</param>
        /// <param name="lastMessageIndex">The index of the last message for this message Id</param>
        /// <param name="messageSegment">The data of the message segment</param>
        public void Add(string msgGroupKey, int messageId, byte messageIndex, byte lastMessageIndex, byte[] messageSegment)
        {
            msgGroupKey = string.IsNullOrEmpty(msgGroupKey) ? string.Empty : msgGroupKey;
            lock (this.valueBufferLock)
            {
                var msgBuffer = this.GetOrCreateMessageBuffer(msgGroupKey, messageId, lastMessageIndex);
                
                if (msgBuffer.ValueBuffer[messageIndex] != null)
                {
                    // We have this segment already ?
                    this.logger.LogTrace("Duplicate message, group key: {0}, msg id: {1}, msg index: {2}", msgGroupKey, messageId, messageIndex);
                }
                else
                {
                    msgBuffer.MessageLength += messageSegment.Length;
                    msgBuffer.ValueBuffer[messageIndex] = messageSegment;
                    msgBuffer.LastUpdate = DateTimeOffset.Now;
                }

                PerformTtlCheck();
            }
        }

        /// <summary>
        /// Returns whether the specified message group key and id combination exists
        /// </summary>
        /// <param name="msgGroupKey">An unique key, which further identifies, which group the message belongs to</param>
        /// <param name="messageId">The message id the segment belongs to</param>
        /// <returns>True if the specified message group key and id combination exists, otherwise false</returns>
        public bool Exists(string msgGroupKey, int messageId)
        {
            lock (this.valueBufferLock)
            {
                return this.msgGroupBuffers.TryGetValue(msgGroupKey, out var groupBuffers) && groupBuffers.Any(x => x != null && x.MessageId == messageId);
            }
        }

        private BufferedValue GetOrCreateMessageBuffer(string msgGroupKey, int messageId, byte lastMessageIndex)
        {
            if (!this.msgGroupBuffers.TryGetValue(msgGroupKey, out var groupBuffers))
            {
                groupBuffers = new BufferedValue[this.bufferPerMessageGroupKey];
                this.msgGroupBuffers[msgGroupKey] = groupBuffers;
            }

            var msgBuffer = groupBuffers.FirstOrDefault(x => x != null && x.MessageId == messageId);
            if (msgBuffer == null)
            {
                // check if there is a free slot
                var indexToUse = Array.IndexOf(groupBuffers, null);
                if (indexToUse == -1)
                {
                    // time to kick out one
                    var kickOut = groupBuffers.OrderBy(x => x.LastUpdate).First();
                    indexToUse = Array.IndexOf(groupBuffers, kickOut);
                    this.logger.LogWarning("Concurrent split message track count reached, dropping oldest msg with segments. Group key: {0}, msg id: {1}", msgGroupKey, kickOut.MessageId);
                    this.OnMessagePurged?.Invoke(new MessagePurgedEventArgs(msgGroupKey, kickOut.MessageId));
                }

                msgBuffer = new BufferedValue(messageId, lastMessageIndex);
                groupBuffers[indexToUse] = msgBuffer;
            }

            return msgBuffer;
        }
        
        private BufferedValue RemoveMessageBuffer(string msgGroupKey, int messageId)
        {
            if (!this.msgGroupBuffers.TryGetValue(msgGroupKey, out var groupBuffers))
            {
                return null;
            }

            var msgBuffer = groupBuffers.FirstOrDefault(x => x != null && x.MessageId == messageId);
            if (msgBuffer == null)
            {
                return null;
            }

            var indexToFree = Array.IndexOf(groupBuffers, msgBuffer);
            groupBuffers[indexToFree] = null; // free it up
            
            // check if the msgGroup is empty, if so, remove
            if (groupBuffers.All(x => x == null))
            {
                this.msgGroupBuffers.Remove(msgGroupKey);
            }
            
            return msgBuffer;
        }

        private void PerformTtlCheck()
        {
            if (this.lastTtlCheck.AddTicks(timeToLive.Ticks / 10) > DateTimeOffset.Now) return; // check max N times per TTL duration to avoid spamming
            this.lastTtlCheck = DateTimeOffset.Now;
            var cutoff = DateTimeOffset.Now - timeToLive;
            foreach (var msgGroupBuffer in msgGroupBuffers)
            {
                for (var index = 0; index < msgGroupBuffer.Value.Length; index++)
                {
                    var msgSegment = msgGroupBuffer.Value[index];
                    if (msgSegment == null) continue;
                    if (msgSegment.LastUpdate > cutoff) continue; // not old enough
                    msgGroupBuffer.Value[index] = null;
                    this.logger.LogWarning("Message segment expired, only a part of the message was received within allowed time. Group key: {0}, msg id: {1}.", msgGroupBuffer.Key, msgSegment.MessageId);
                    this.OnMessagePurged?.Invoke(new MessagePurgedEventArgs(msgGroupBuffer.Key, msgSegment.MessageId));
                }
            }
            
            // clear up ones with only null buffers
            var keysToRemove = msgGroupBuffers.Where(x => x.Value.All(bv => bv == null)).Select(x => x.Key).ToList();
            foreach (var key in keysToRemove)
            {
                msgGroupBuffers.Remove(key);
            }
        }

        /// <summary>
        /// Removes the message segments from the buffer and returns them in the order according to their message index
        /// </summary>
        /// <param name="msgGroupKey">A unique key, which further identifies to which group the message belongs</param>
        /// <param name="messageId"></param>
        /// <param name="segmentLengths">
        /// It is set to 0 if message is not in buffer, else to the data length of the segments for the message id
        /// </param>
        /// <param name="segmentCount">It is set to 0 if message is not in buffer, else to the segment count for the message id</param>
        /// <returns>The messages segments in order. Returns zero length if requested message segments are unavailable</returns>
        public IReadOnlyList<byte[]> Remove(string msgGroupKey, int messageId, out int segmentLengths, out byte segmentCount)
        {
            msgGroupKey = string.IsNullOrEmpty(msgGroupKey) ? string.Empty : msgGroupKey;
            lock (this.valueBufferLock)
            {
                var buffer = RemoveMessageBuffer(msgGroupKey, messageId);
                PerformTtlCheck();
                if (buffer == null)
                {
                    segmentLengths = 0;
                    segmentCount = 0;
                    return null;
                }

                segmentLengths = buffer.MessageLength;
                segmentCount = (byte) buffer.ValueBuffer.Length;

                var val = buffer.ValueBuffer;
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
            public readonly string MessageGroupKey;

            /// <summary>
            /// Message Id
            /// </summary>
            public readonly int MessageId;

            /// <summary>
            /// Initializes a new instance of <see cref="MessagePurgedEventArgs"/>
            /// </summary>
            /// <param name="messageGroupKey">Message group key</param>
            /// <param name="messageId">Message Id</param>
            public MessagePurgedEventArgs(string messageGroupKey, int messageId)
            {
                this.MessageGroupKey = messageGroupKey;
                this.MessageId = messageId;
            }
        }
    }
}