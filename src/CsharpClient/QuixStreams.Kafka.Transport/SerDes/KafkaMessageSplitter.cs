using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.Kafka.Transport.SerDes.Legacy;

namespace QuixStreams.Kafka.Transport.SerDes
{
    /// <summary>
    /// Splits Kafka messages into multiple message
    /// </summary>
    public interface IKafkaMessageSplitter
    {
        /// <summary>
        /// Splits the kafka message
        /// </summary>
        /// <param name="message">The kafka message to split</param>
        /// <returns>The transport packages resulting from the split</returns>
        IEnumerable<KafkaMessage> Split(KafkaMessage message);

        /// <summary>
        /// Returns whether the message should be split according to implementation logic
        /// </summary>
        /// <param name="message">The message to check if needs splitting</param>
        /// <returns>Whether splitting is required</returns>
        bool ShouldSplit(KafkaMessage message);
    }

    /// <summary>
    /// Splits Kafka messages into multiple message
    /// </summary>
    public class KafkaMessageSplitter : IKafkaMessageSplitter
    {
        /// <summary>
        /// The maximum size of a kafka message including header, key, value 
        /// </summary>
        internal readonly int MaximumKafkaMessageSize;
        
        /// <summary>
        /// The amount of segments at which a warning message should be logged
        /// </summary>
        private const int VerboseWarnAboveSegmentCount = 3;
        
        /// <summary>
        /// The message size above which a warning message should be logged. This is a moving size to avoid
        /// constant spamming of warning
        /// </summary>
        private static int MovingWarnAboveSize = 0;
        
        private static readonly ILogger logger = QuixStreams.Logging.CreateLogger(typeof(KafkaMessageSplitter));

        /// <summary>
        /// The expected size of the the details to describe the split info
        /// </summary>
        internal static int ExpectedHeaderSplitInfoSize = KafkaMessage.EstimateHeaderSize(
            CreateSegmentDictionary(int.MaxValue, BitConverter.GetBytes(int.MaxValue), Guid.NewGuid().ToByteArray()));

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaMessageSplitter"/>
        /// </summary>
        /// <param name="maximumKafkaMessageSize">The maximum message size kafka accepts</param>
        public KafkaMessageSplitter(int maximumKafkaMessageSize)
        {
            this.MaximumKafkaMessageSize = maximumKafkaMessageSize;
        }
        
        /// <summary>
        /// Splits the kafka message
        /// </summary>
        /// <param name="message">The kafka message to split</param>
        /// <returns>The transport packages resulting from the split</returns>
        public IEnumerable<KafkaMessage> Split(KafkaMessage message)
        {
            if (message.MessageSize <= this.MaximumKafkaMessageSize)
            {
                yield return message;
                yield break;
            }

            var segmentCount = 0;
            if (PackageSerializationSettings.Mode == PackageSerializationMode.LegacyValue)
            {
                foreach (var splitMessage in LegacySplit(message))
                {
                    segmentCount++;
                    yield return splitMessage;
                }
                WarningCheck(segmentCount, message.Value.Length);
                yield break;
            }
            
            foreach (var splitMessage in HeaderSplit(message))
            {
                segmentCount++;
                yield return splitMessage;
            }
            WarningCheck(segmentCount, message.Value.Length);
        }

        /// <inheritdoc/>
        public bool ShouldSplit(KafkaMessage message)
        {
            if (message == null) return false;
            return message.MessageSize > this.MaximumKafkaMessageSize;
        }


        private IEnumerable<KafkaMessage> HeaderSplit(KafkaMessage message)
        {
            var valueSizeMax = this.MaximumKafkaMessageSize - message.HeaderSize - (message.Key?.Length ?? 0);
            valueSizeMax -= ExpectedHeaderSplitInfoSize;

            if (valueSizeMax < 1) // The message can't realistically be split, so just return it and log a warning
            {
                logger.LogWarning("A message could not be split because your message limit is {0} bytes.", this.MaximumKafkaMessageSize);
                yield return message;
            }

            var messageId = Guid.NewGuid().ToByteArray();
            var count = (int)Math.Ceiling((double)message.Value.Length / valueSizeMax);
            var countAsBytes = BitConverter.GetBytes(count);
            var start = 0;
            for (int index = 0; index < count; index++)
            {
                var end = Math.Min(start + valueSizeMax, message.Value.Length); // non inclusive
                var length = end - start;
                var segment = new byte[length];
                Array.Copy(message.Value, start, segment, 0, length);
                var headers = CreateSegmentDictionary(index, countAsBytes, messageId);
                var messageHeaders = message.Headers?.ToList() ?? new List<KafkaHeader>();
                foreach (var header in headers)
                {
                    messageHeaders.Add(new KafkaHeader(header.Key, header.Value));
                }

                var segmentMessage = new KafkaMessage(message.Key, segment, messageHeaders.ToArray());
                yield return segmentMessage;
                start = end;
            }
        }

        internal static IDictionary<string, byte[]> CreateSegmentDictionary(int index, byte[] countBytes, byte[] messageGuidBytes)
        {
            return new Dictionary<string, byte[]>
            {
                { Constants.KafkaMessageHeaderSplitMessageId, messageGuidBytes },
                { Constants.KafkaMessageHeaderSplitMessageCount, countBytes },
                { Constants.KafkaMessageHeaderSplitMessageIndex, BitConverter.GetBytes(index) }
            };
        }

        private IEnumerable<KafkaMessage> LegacySplit(KafkaMessage message)
        {
            var valueSizeMax = this.MaximumKafkaMessageSize - message.HeaderSize - (message.Key?.Length ?? 0);
            
            foreach (var segment in LegacyByteSplitter.Split(message.Value, valueSizeMax))
            {
                yield return new KafkaMessage(message.Key, segment, message.Headers);
            }
        }

        private void WarningCheck(int segmentCount, int messageLength)
        {
            if (segmentCount <= VerboseWarnAboveSegmentCount) return;
                
            if (messageLength > MovingWarnAboveSize)
            {
                // not thread safe, but better than having too many warnings or some performance implication 
                MovingWarnAboveSize = Math.Max(messageLength, MovingWarnAboveSize) * 2;
                logger.LogWarning("One or more of your messages exceed the optimal size. Consider publishing smaller for better consumer experience. Your message was over {0}KB.", Math.Round((double)messageLength/1024, 1));
            }
            else
            {
                logger.LogTrace("One or more of your messages exceed the optimal size. Consider publishing smaller for better consumer experience. Your message was over {0}KB.", Math.Round((double)messageLength/1024, 1));
            }
        }
    }
}