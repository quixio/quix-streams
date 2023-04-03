using System;
using FluentAssertions;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{
    public class TopicConsumerTests
    {
        private TestStreamingClient CreateTestClient()
        {
            return new TestStreamingClient();
        }
        
        [Fact]
        public void GetTopicState_ShouldReturnSameState()
        {
            // Arrange
            var client = CreateTestClient();
            var consumer = client.GetTopicConsumer();

            // Act
            var firstState = consumer.GetState<int>("mystate");
            var secondState = consumer.GetState<int>("mystate");

            // Assert
            firstState.Should().BeSameAs(secondState);
        }
        
        [Fact]
        public void GetTopicState_WithDifferentType_ShouldThrowException()
        {
            // Arrange
            var client = CreateTestClient();
            var consumer = client.GetTopicConsumer();
            var firstState = consumer.GetState<int>("mystate");

            // Act
            Action action = () => consumer.GetState<long>("mystate");

            // Assert
            action.Should().Throw<ArgumentException>().WithMessage("*already exists with a different type*");
        }
    }
}