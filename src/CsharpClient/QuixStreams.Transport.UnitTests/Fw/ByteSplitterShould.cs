using System;
using System.Linq;
using System.Runtime.Serialization;
using FluentAssertions;
using QuixStreams.Transport.Fw;
using Xunit;

namespace QuixStreams.Transport.UnitTests.Fw
{
    public class ByteSplitterShould
    {
        [Fact]
        public void Constr_WithTooLowMaxMessageSize_ShouldThrowArgumentOutOfRangException()
        {
            Action action = () => new ByteSplitter(7);
            action.Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Split_WithDataOutsideAbsoluteMaxSize_ShouldThrowSerializationException()
        {
            // Arrange
            var splitter = new ByteSplitter(50);
            var length = splitter.AbsoluteMaxMessageSize + 1;
            var data = new byte[length];
            var random = new Random();
            random.NextBytes(data);

            // Act
            Action action = () => splitter.Split(data).ToList();

            // Assert
            action.Should().Throw<SerializationException>();
        }

        [Fact]
        public void Split_WithDataOutsideAllowedMessageSizeButWithinAbsoluteMaxSize_ShouldReturnSplitBytes()
        {
            // Arrange
            const int maxMsgLength = 50;
            var splitter = new ByteSplitter(maxMsgLength);
            var length = splitter.AbsoluteMaxMessageSize - 10; // just a bit less than max;
            var data = new byte[length];
            var random = new Random();
            random.NextBytes(data);

            // Act
            var segments = splitter.Split(data).ToList();

            // Assert
            var expectedMaxMessageIndex = (byte) (byte.MaxValue - 1);
            segments.Count.Should().Be(expectedMaxMessageIndex + 1);
            var expectedMessageId = -1;
            var dataByteIndex = 0;
            for (var index = 0; index < segments.Count; index++)
            {
                var segment = segments[index];
                ByteSplitter.TryGetSplitDetails(segment, out var messageId, out var messageIndex, out var lastMessageIndex, out var messageData)
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
                lastMessageIndex.Should().Be(expectedMaxMessageIndex);

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
        public void Split_WithDataWithAbsoluteMaxSize_ShouldNotThrowSerializationException()
        {
            // Arrange
            var splitter = new ByteSplitter(50);
            var length = splitter.AbsoluteMaxMessageSize;
            var data = new byte[length];
            var random = new Random();
            random.NextBytes(data);

            // Act
            var result = splitter.Split(data).ToList();

            // Assert by not having an exception thrown
        }

        [Fact]
        public void Split_WithDataWithinAllowedMessageSize_ShouldReturnSameBytes()
        {
            // Arrange
            var splitter = new ByteSplitter(50);
            var data = new byte[48];
            var random = new Random();
            random.NextBytes(data);

            // Act
            var segments = splitter.Split(data).ToList();

            // Assert
            segments.Count.Should().Be(1, "Bytes should not be split");
            segments[0].Should().BeSameAs(data);
        }
    }
}