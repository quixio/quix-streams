using FluentAssertions;
using Quix.Sdk.Process.Models;
using Xunit;

namespace Quix.Sdk.Streaming.UnitTests
{
    public class StreamReaderShould
    {
        [Fact]
        public void Close_ShouldRaiseTerminatedCloseType()
        {
            // Arrange
            var streamReader = new StreamReader(new TestStreamingClient().CreateInputTopic(), "asdf");
            StreamEndType? endType = null;
            streamReader.OnStreamClosed += (sender, args) =>
            {
                endType = args.EndType;
            };

            // Act
            streamReader.Close();

            // Assert
            endType.Should().Be(StreamEndType.Terminated);
        }
        
        [Fact]
        public void Dispose_ShouldRaiseTerminatedCloseType()
        {
            // Arrange
            var streamReader = new StreamReader(new TestStreamingClient().CreateInputTopic(), "asdf");
            StreamEndType? endType = null;
            streamReader.OnStreamClosed += (sender, args) =>
            {
                endType = args.EndType;
            };

            // Act
            streamReader.Dispose();

            // Assert
            endType.Should().Be(StreamEndType.Terminated);
        }
        
        [Fact]
        public void SendStreamEnd_ShouldRaiseExpectedCloseType()
        {
            // Arrange
            var streamReader = new StreamReader(new TestStreamingClient().CreateInputTopic(), "asdf");
            StreamEndType? endType = null;
            streamReader.OnStreamClosed += (sender, args) =>
            {
                endType = args.EndType;
            };

            // Act
            streamReader.Send(new StreamEnd() {StreamEndType = StreamEndType.Aborted});

            // Assert
            endType.Should().Be(StreamEndType.Aborted);
        }
        
        [Fact]
        public void SendStreamEndAndClose_ShouldRaiseOnlyStreamEndCloseType()
        {
            // Arrange
            var streamReader = new StreamReader(new TestStreamingClient().CreateInputTopic(), "asdf");
            StreamEndType? endType = null;
            var closeCount = 0;
            streamReader.OnStreamClosed += (sender, args) =>
            {
                closeCount++;
                endType = args.EndType;
            };

            // Act
            streamReader.Send(new StreamEnd() {StreamEndType = StreamEndType.Aborted});
            streamReader.Close();

            // Assert
            closeCount.Should().Be(1);
            endType.Should().Be(StreamEndType.Aborted);
        }
    }
}