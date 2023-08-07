using FluentAssertions;
using QuixStreams.Streaming.UnitTests.Helpers;
using QuixStreams.Telemetry.Models;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{
    public class StreamConsumerShould
    {
        [Fact]
        public void Close_ShouldRaiseTerminatedCloseType()
        {
            // Arrange
            var streamConsumer = new StreamConsumer(new TestStreamingClient().GetTopicConsumer(), "asdf");
            StreamEndType? endType = null;
            streamConsumer.OnStreamClosed += (sender, args) =>
            {
                endType = args.EndType;
            };

            // Act
            streamConsumer.Close();

            // Assert
            endType.Should().Be(StreamEndType.Terminated);
        }
        
        [Fact]
        public void Dispose_ShouldRaiseTerminatedCloseType()
        {
            // Arrange
            var streamConsumer = new StreamConsumer(new TestStreamingClient().GetTopicConsumer(), "asdf");
            StreamEndType? endType = null;
            streamConsumer.OnStreamClosed += (sender, args) =>
            {
                endType = args.EndType;
            };

            // Act
            streamConsumer.Dispose();

            // Assert
            endType.Should().Be(StreamEndType.Terminated);
        }
        
        [Fact]
        public void SendStreamEnd_ShouldRaiseExpectedCloseType()
        {
            // Arrange
            var streamConsumer = new StreamConsumer(new TestStreamingClient().GetTopicConsumer(), "asdf");
            StreamEndType? endType = null;
            streamConsumer.OnStreamClosed += (sender, args) =>
            {
                endType = args.EndType;
            };

            // Act
            streamConsumer.Send(new StreamEnd() {StreamEndType = StreamEndType.Aborted});

            // Assert
            endType.Should().Be(StreamEndType.Aborted);
        }
        
        [Fact]
        public void SendStreamEndAndClose_ShouldRaiseOnlyStreamEndCloseType()
        {
            // Arrange
            var streamConsumer = new StreamConsumer(new TestStreamingClient().GetTopicConsumer(), "asdf");
            StreamEndType? endType = null;
            var closeCount = 0;
            streamConsumer.OnStreamClosed += (sender, args) =>
            {
                closeCount++;
                endType = args.EndType;
            };

            // Act
            streamConsumer.Send(new StreamEnd() {StreamEndType = StreamEndType.Aborted});
            streamConsumer.Close();

            // Assert
            closeCount.Should().Be(1);
            endType.Should().Be(StreamEndType.Aborted);
        }
    }
}