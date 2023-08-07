using System.Linq;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.States;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States
{
    public class TopicStateManagerShould
    {
        private TopicStateManager CreateTopicStateManager()
        {
            return new TopicStateManager("topic", new InMemoryStorage(), NullLoggerFactory.Instance);
        }
    
        [Fact]
        public void GetStreamStates_ShouldReturnExcepted()
        {
            // Arrange
            var manager = CreateTopicStateManager();

            // Assert
            var states = manager.GetStreamStates().ToArray();
            states.Length.Should().Be(0);
        
            // Act
            manager.GetStreamStateManager("test");
            manager.GetStreamStateManager("test2");
            manager.GetStreamStateManager("test2"); // get it twice
            manager.GetStreamStateManager("test3");
            manager.DeleteStreamState("test3");
        
            // Assert
            states = manager.GetStreamStates().ToArray();
            states.Should().BeEquivalentTo(new[] { "test", "test2" });
        }
    
        [Fact]
        public void GetStreamStateManager_ShouldReturnSameManagerAlways()
        {
            // Arrange
            var manager = CreateTopicStateManager();
        
            // Act
            var testTopicStateManager = manager.GetStreamStateManager("test");
            var testTopicStateManager2 = manager.GetStreamStateManager("test");
        
            // Assert
            testTopicStateManager.Should().NotBeNull();
            testTopicStateManager.Should().Be(testTopicStateManager2);
        }
    
        [Fact]
        public void DeleteStreamState_ShouldReturnExpected()
        {
            // Arrange
            var manager = CreateTopicStateManager();
        
            // Act & Assert
            manager.DeleteStreamState("test").Should().BeFalse();
            var testTopicStateManager = manager.GetStreamStateManager("test");
            manager.DeleteStreamState("test").Should().BeTrue();
            var testTopicStateManager2 = manager.GetStreamStateManager("test");
        
            testTopicStateManager2.Should().NotBeNull();
            testTopicStateManager2.Should().NotBe(testTopicStateManager);
        }
    
        [Fact]
        public void DeleteStreamStates_ShouldReturnExpected()
        {
            // Arrange
            var manager = CreateTopicStateManager();
            var testTopicStateManager = manager.GetStreamStateManager("test");
            var testTopicStateManager2 = manager.GetStreamStateManager("test2");
        
            // Act 
            var count = manager.DeleteStreamStates();
        
            // Arrange
            count.Should().Be(2);
            manager.GetStreamStates().Should().BeEmpty();
            manager.GetStreamStateManager("test").Should().NotBe(testTopicStateManager);
        }
    }
}