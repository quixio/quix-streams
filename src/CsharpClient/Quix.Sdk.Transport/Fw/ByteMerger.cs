using System;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport.Fw
{
    /// <summary>
    /// Extensions methods for IByteMerger
    /// </summary>
    public static class ByteMergerExtensions
    {
        /// <summary>
        ///     Buffers and merges message segments created using <see cref="ByteSplitter.Split" />
        /// </summary>
        /// <param name="byteMerger">Byte Merger instance</param>
        /// <param name="bytePackage">Byte array package</param>
        /// <param name="bufferId">The id generated to track the group this message belongs to for buffering reasons. Null when no buffering occurs</param> 
        /// <returns><see cref="T:byte[]" /> if requires no merge or successfully merged, else null</returns>
        public static byte[] Merge(this IByteMerger byteMerger, Package<byte[]> bytePackage, out string bufferId)
        {
            bufferId = null;
            if (bytePackage == null) return null;
            var packageBytes = bytePackage.Value.Value;
            bytePackage.TransportContext.TryGetTypedValue<string>(KnownTransportContextKeys.MessageGroupKey, out var key);
            return byteMerger.Merge(packageBytes, key, out bufferId);
        }
    }

    /// <summary>
    ///     Merges messages split by <see cref="IByteSplitter" />
    /// </summary>
    public interface IByteMerger
    {
        /// <summary>
        ///     Buffers and merges message segments created using <see cref="ByteSplitter.Split" />
        /// </summary>
        /// <param name="messageSegment">The segment created using <see cref="ByteSplitter.Split" /></param>
        /// <param name="msgGroupKey">An unique key, which further identifies, which group the message belongs to. Optional</param>
        /// <param name="bufferId">The id generated to track the group this message belongs to for buffering reasons. Null when no buffering occurs</param> 
        /// <returns><see cref="T:byte[]" /> if requires no merge or successfully merged, else null</returns>
        byte[] Merge(byte[] messageSegment, string msgGroupKey, out string bufferId);

        /// <summary>
        /// Removes all buffered data for the specified buffer Id
        /// </summary>
        /// <param name="bufferId">The buffer id to purge</param>
        void Purge(string bufferId);

        /// <summary>
        /// Raised when message segments of the specified buffer id have been purged. Reason could be timout or similar.
        /// </summary>
        event Action<string> OnMessageSegmentsPurged;
    }

    /// <summary>
    ///     Merges messages split by <see cref="ByteSplitter" />
    /// </summary>
    public class ByteMerger : IByteMerger
    {
        private readonly MergeBuffer buffer;
        private readonly ILogger logger = Logging.CreateLogger<ByteMerger>();

        /// <summary>
        ///     Initializes a new instance of <see cref="ByteMerger" />
        /// </summary>
        /// <param name="buffer">The buffer to use to store the message parts</param>
        public ByteMerger(MergeBuffer buffer)
        {
            this.buffer = buffer;
            this.buffer.OnMessagePurged += (ea) =>
            {
                this.OnMessageSegmentsPurged?.Invoke($"{ea.MessageGroupKey}-{ea.MessageId}");
            };
        }

        /// <inheritdoc />
        public byte[] Merge(byte[] messageSegment, string msgGroupKey, out string bufferId)
        {
            // is this even a split message ?
            if (!ByteSplitter.TryGetSplitDetails(messageSegment, out var msgId, out var msgIndex, out var lastMsgIndex, out var msgData))
            {
                bufferId = null;
                return messageSegment;
            }

            this.buffer.Add(msgGroupKey, msgId, msgIndex, lastMsgIndex, msgData);
            bufferId = $"{msgGroupKey}-{msgId}";
            if (msgIndex != lastMsgIndex)
            {
                // we do not have all parts yet if assuming proper message order
                return null;
            }

            // last message! Lets check if we have all parts;

            return this.AssembleMessage(msgId, msgGroupKey, ref bufferId);
        }

        /// <inheritdoc />
        public void Purge(string bufferId)
        {
            if (string.IsNullOrWhiteSpace(bufferId)) throw new ArgumentNullException(nameof(bufferId));
            var lastDash = bufferId.LastIndexOf('-');
            if (lastDash == -1) throw new ArgumentOutOfRangeException(nameof(bufferId), $"Invalid buffer id '{bufferId}' provided. Missing msg id component.");
            if (!int.TryParse(bufferId.Substring(lastDash + 1), out var msgId))
            {
                throw new ArgumentOutOfRangeException(nameof(bufferId), $"Invalid buffer id '{bufferId}' provided. MsgId could not be parsed.");
            }

            var msgGroupKey = bufferId.Substring(0, lastDash);
            this.buffer.Remove(msgGroupKey, msgId, out _, out var count);
        }

        /// <inheritdoc />
        public event Action<string> OnMessageSegmentsPurged;

        private byte[] AssembleMessage(int msgId, string msgGroupKey, ref string msgGroupId)
        {
            var completedBuffer = this.buffer.Remove(msgGroupKey, msgId, out var bufferSize, out var segmentCount);
            if (segmentCount == 0)
            {
                // message never was or no longer in buffer !
                logger.LogDebug("Received last segment for {0}, but rest of the segments can no longer be found.", msgGroupId);
                msgGroupId = null;
                return null;
            }

            // Merge data into buffer
            var msgBuffer = new byte[bufferSize];
            var destinationIndex = 0;
            for (var index = 0; index < completedBuffer.Count; index++)
            {
                var msgValueBytes = completedBuffer[index];
                if (msgValueBytes == null)
                {
                    logger.LogDebug("Received last segment for {0}, but some segments can no longer be found.", msgGroupId);
                    msgGroupId = null;
                    return null;
                }
                Array.Copy(msgValueBytes, 0, msgBuffer, destinationIndex, msgValueBytes.Length);
                destinationIndex += msgValueBytes.Length;
            }

            return msgBuffer;
        }
    }
}