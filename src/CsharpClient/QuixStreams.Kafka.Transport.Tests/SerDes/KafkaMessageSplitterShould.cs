using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.SerDes.Legacy;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes
{
    public class KafkaMessageSplitterShould
    {
        [Fact]
        public void Split_WithDataOutsideAbsoluteMaxSize_ShouldThrowSerializationException()
        {
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = PackageSerializationMode.LegacyValue;
            const int maxMsgLength = 50;
            var splitter = new KafkaMessageSplitter(maxMsgLength);
            var length = maxMsgLength * byte.MaxValue + 10;
            var data = new byte[length];
            var random = new Random();
            random.NextBytes(data);

            var msg = new KafkaMessage(null, data, null);

            // Act
            Action action = () => splitter.Split(msg).ToList();

            // Assert
            action.Should().Throw<SerializationException>();
        }

        [Fact]
        public void SplitLegacy_WithDataOutsideAllowedMessageSizeButWithinAbsoluteMaxSize_ShouldReturnSplitBytes()
        {
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = PackageSerializationMode.LegacyValue;
            const int maxMsgLength = 50;
            var splitter = new KafkaMessageSplitter(maxMsgLength);
            var length = 10199; // just a bit less than max (10200), but greater because of split info.
            var data = new byte[length];
            var random = new Random();
            random.NextBytes(data);

            var msg = new KafkaMessage(null, data, null);
            // Act
            var segments = splitter.Split(msg).ToList();

            // Assert
            var expectedMessageCount = (byte) (byte.MaxValue - 1);
            segments.Count.Should().Be(expectedMessageCount + 1);
            var expectedMessageId = -1;
            var dataByteIndex = 0;
            for (var index = 0; index < segments.Count; index++)
            {
                var segment = segments[index];
                LegacyByteSplitter.TryGetSplitDetails(segment.Value, out var messageId, out var messageIndex, out var lastMessageIndex, out var messageData)
                    .Should().BeTrue($"Segment {index} should be a message segment");

                if (index == 0)
                {
                    messageId.Should().NotBe(-1);
                    expectedMessageId = messageId;
                }
                else
                {
                    messageId.Should().Be(expectedMessageId);
                }

                messageIndex.Should().Be((byte) index);
                lastMessageIndex.Should().Be(expectedMessageCount);

                for (var i = 0; i < messageData.Length; i++)
                {
                    messageData[i].Should().Be(data[dataByteIndex],
                        $"Message {messageIndex}/{lastMessageIndex} at {i}/{messageData.Length} should have be equivalent to {dataByteIndex}/{data.Length} in original data");
                    dataByteIndex++;
                }
            }

            dataByteIndex.Should().Be(data.Length); // due to last increment, it should actually match the length
        }
        
        [Fact]
        public void Split_GreaterThanSupportedByLegacy_ShouldReturnSplitBytes()
        {
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = PackageSerializationMode.Header; 
            int maxMsgLength = KafkaMessageSplitter.ExpectedHeaderSplitInfoSize + 50;
            var splitter = new KafkaMessageSplitter(maxMsgLength);
            var length = 40000; // legacy would die around 10200 in this config
            var data = new byte[length];
            var random = new Random();
            random.NextBytes(data);

            var msg = new KafkaMessage(null, data, null);
            // Act
            var segments = splitter.Split(msg).ToList();

            // Assert
            var expectedMessageCount = 800; // 40K/50;
            segments.Count.Should().Be(expectedMessageCount);
            string expectedMessageId = string.Empty;
            var dataByteIndex = 0;
            for (var index = 0; index < segments.Count; index++)
            {
                var segment = segments[index];
                var messageId = Constants.Utf8NoBOMEncoding.GetString(segment.Headers.First(y=> y.Key == Constants.KafkaMessageHeaderSplitMessageId).Value);
                var messageIndex = BitConverter.ToInt32(segment.Headers.First(y=> y.Key == Constants.KafkaMessageHeaderSplitMessageIndex).Value, 0);
                var messageCount = BitConverter.ToInt32(segment.Headers.First(y=> y.Key == Constants.KafkaMessageHeaderSplitMessageCount).Value, 0);
                if (index == 0)
                {
                    expectedMessageId = messageId;
                }
                else
                {
                    messageId.Should().Be(expectedMessageId);
                }

                messageIndex.Should().Be(index);
                messageCount.Should().Be(expectedMessageCount);

                for (var i = 0; i < segment.Value.Length; i++)
                {
                    var expected = data[dataByteIndex];
                    var actual = segment.Value[i];
                    actual.Should().Be(expected,
                        $"Message {messageIndex}/{messageCount} at {i}/{segment.Value.Length} should have be equivalent to {dataByteIndex}/{data.Length} in original data");
                    dataByteIndex++;
                }
            }

            dataByteIndex.Should().Be(data.Length); // due to last increment, it should actually match the length
        }

        [Theory]
        [InlineData(PackageSerializationMode.Header)]
        [InlineData(PackageSerializationMode.LegacyValue)]
        public void Split_WithDataWithinAllowedMessageSize_ShouldReturnSameBytes(PackageSerializationMode mode)
        {
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = mode;
            var splitter = new KafkaMessageSplitter(50);
            var data = new byte[50];
            var random = new Random();
            random.NextBytes(data);
            var message = new KafkaMessage(null, data, null);

            // Act
            var segments = splitter.Split(message).ToList();

            // Assert
            segments.Count.Should().Be(1, "Bytes should not be split");
            segments[0].Value.Should().BeSameAs(data);
        }
        
        [Theory]
        [InlineData(PackageSerializationMode.Header)]
        [InlineData(PackageSerializationMode.LegacyValue)]
        public void Split_WithDataWithinAllowedMessageSizeUsingHeaderAndKey_ShouldReturnSameBytes(PackageSerializationMode mode)
        {
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = mode;
            var splitter = new KafkaMessageSplitter(50);
            var data = new byte[5];
            var random = new Random();
            random.NextBytes(data);
            var key = new byte[5];
            random.NextBytes(key);
            var header = new [] { new KafkaHeader("test", new byte[4])};
            var message = new KafkaMessage(key, data, header);

            // Act
            var segments = splitter.Split(message).ToList();

            // Assert
            segments.Count.Should().Be(1, "Bytes should not be split");
            segments[0].Value.Should().BeSameAs(data);
        }
    }
}