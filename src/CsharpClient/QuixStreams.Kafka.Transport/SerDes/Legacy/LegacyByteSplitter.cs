using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading;
using static QuixStreams.Kafka.Transport.SerDes.Constants;

namespace QuixStreams.Kafka.Transport.SerDes.Legacy
{
    /// <summary>
    /// LEGACY Serialization for Transport Package Value when the data used to be encoded inside the message's value
    /// Splits messages for publishing onto transport layer according to limitations. Can be merged by <see cref="LegacyByteMerger" />
    /// </summary>
    internal static class LegacyByteSplitter
    {
        /// <summary>
        /// The only reason this class exists is because mono is having trouble with generic IEnumerable&lt;Byte[]&gt; using iterator implementation
        /// </summary>
        private class SplitEnumerator : IEnumerator<byte[]>
        {
            private readonly byte[] msgBytes;
            private readonly long messageId;
            private readonly byte maxIndex;
            private readonly int plannedMessageSize;            
            private byte index;

            public SplitEnumerator(byte[] msgBytes, int maxMessageSize, long messageId)
            {
                this.msgBytes = msgBytes;
                this.messageId = messageId;


                if (msgBytes.Length <= maxMessageSize)
                {
                    this.maxIndex = 0;
                    return;
                }
                this.plannedMessageSize = maxMessageSize - LegacyMessageSeparatorInfoLength;
                this.maxIndex = (byte)(Math.Ceiling((double)msgBytes.Length / plannedMessageSize) - 1);
            }

            public bool MoveNext()
            {
                if (this.index > maxIndex) return false;

                if (this.maxIndex == 0)
                {
                    this.Current = msgBytes;
                    index++;
                    return true;
                }

                // because message value itself is evaluated at a later point to save memory, need to store few things in local context
                var msgStartIndex = this.index * plannedMessageSize;
                var remainingBytes = msgBytes.Length - msgStartIndex;

                var msgSegmentLength = Math.Min(remainingBytes, plannedMessageSize);
                var newMsgBytes =  new byte[msgSegmentLength + LegacyMessageSeparatorInfoLength];
                // copy the messageData inside the split message
                Array.Copy(msgBytes, msgStartIndex, newMsgBytes, LegacyMessageSeparatorInfoLength, msgSegmentLength);
                // now set the message split messageData
                newMsgBytes[0] = SplitStart;
                var msgIdBytes = BitConverter.GetBytes(this.messageId);
                newMsgBytes[1] = msgIdBytes[0];
                newMsgBytes[2] = msgIdBytes[1];
                newMsgBytes[3] = msgIdBytes[2];
                newMsgBytes[4] = msgIdBytes[3];
                newMsgBytes[5] = SplitSeparator;
                newMsgBytes[6] = index;
                newMsgBytes[7] = SplitSeparator;
                newMsgBytes[8] = maxIndex;
                newMsgBytes[9] = SplitEnd;
                this.Current = newMsgBytes;
                index++;
                return true;
            }

            public void Reset()
            {
                this.index = 0;
            }

            public byte[] Current { get; set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                
            }
        }

        private class SplitEnumerable : IEnumerable<byte[]>
        {
            private readonly SplitEnumerator enumerator;

            public SplitEnumerable(SplitEnumerator enumerator)
            {
                this.enumerator = enumerator;
            }
            
            public IEnumerator<byte[]> GetEnumerator()
            {
                return this.enumerator;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
        
        private static long messageId;


        /// <summary>
        /// Splits the given byte array into messages defined by the max message size
        /// </summary>
        /// <param name="msgBytes">The message bytes</param>
        /// <param name="maxMessageSize">The maximum message size</param>
        /// <returns>The split message fragments</returns>
        /// <exception cref="ArgumentOutOfRangeException">When the max message size is less than 11</exception>
        /// <exception cref="SerializationException">When the message is too big to split with this splitter</exception>
        public static IEnumerable<byte[]> Split(byte[] msgBytes, int maxMessageSize)
        {
            if (maxMessageSize < LegacyMessageSeparatorInfoLength + 1)
            {
                throw new ArgumentOutOfRangeException(nameof(maxMessageSize), $"Max message size must be at least {LegacyMessageSeparatorInfoLength + 1}. Was given {maxMessageSize}.");
            }
            
            var absoluteMaxMessageSize = maxMessageSize * byte.MaxValue - byte.MaxValue * LegacyMessageSeparatorInfoLength;

            
            if (msgBytes.Length > absoluteMaxMessageSize)
            {
                throw new SerializationException($"Message size {msgBytes.Length} bytes exceeds allowed maximum message size of {absoluteMaxMessageSize} bytes");
            }

            var messageId = Interlocked.Increment(ref LegacyByteSplitter.messageId);
            return new SplitEnumerable(new SplitEnumerator(msgBytes, maxMessageSize, messageId));
        }  

        /// <summary>
        /// Gets the split details from the split.
        /// </summary>
        /// <param name="messageSegment">The segment created using <see cref="Split" /></param>
        /// <param name="messageId">The message id contained in the split</param>
        /// <param name="messageIndex">The index of the segment within the message</param>
        /// <param name="lastMessageIndex">The index of the last message segment</param>
        /// <param name="messageData">The message segment messageData</param>
        /// <returns><c>True</c> if the given messageSegment is a split segment, else <c>false</c></returns>
        public static bool TryGetSplitDetails(byte[] messageSegment, out int messageId, out byte messageIndex, out byte lastMessageIndex, out byte[] messageData)
        {
            messageId = 0;
            messageIndex = 0;
            lastMessageIndex = 0;
            messageData = null;
            // is this even a split message ?
            if (messageSegment.Length < LegacyMessageSeparatorInfoLength)
            {
                // not long enough
                return false;
            }

            // check the split char bytes
            if (messageSegment[0] != SplitStart) return false;
            if (messageSegment[5] != SplitSeparator) return false;
            if (messageSegment[7] != SplitSeparator) return false;
            if (messageSegment[9] != SplitEnd) return false;

            messageId = BitConverter.ToInt32(messageSegment, 1);
            messageIndex = messageSegment[6];
            lastMessageIndex = messageSegment[8];

            var dataLength = messageSegment.Length - LegacyMessageSeparatorInfoLength;
            messageData = new byte[dataLength];
            Array.Copy(messageSegment, LegacyMessageSeparatorInfoLength, messageData, 0, dataLength);
            return true;
        }
    }
}