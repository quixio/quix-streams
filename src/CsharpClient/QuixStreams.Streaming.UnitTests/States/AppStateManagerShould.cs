using System.Linq;
using FluentAssertions;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.States;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States
{
    public class AppStateManagerShould
    {
        private AppStateManager CreateAppStateManager()
        {
            return new AppStateManager(new InMemoryStorage());
        }
    
        [Fact]
        public void GetTopicStates_ShouldReturnExcepted()
        {
            // Arrange
            var manager = CreateAppStateManager();

            // Assert
            var states = manager.GetTopicStates().ToArray();
            states.Length.Should().Be(0);
        
            // Act
            manager.GetTopicStateManager("test");
            manager.GetTopicStateManager("test2");
            manager.GetTopicStateManager("test2"); // get it twice
            manager.GetTopicStateManager("test3");
            manager.DeleteTopicState("test3");
        
            // Assert
            states = manager.GetTopicStates().ToArray();
            states.Should().BeEquivalentTo(new[] { "test", "test2" });
        }
    
        [Fact]
        public void GetTopicStateManager_ShouldReturnSameManagerAlways()
        {
            // Arrange
            var manager = CreateAppStateManager();
        
            // Act
            var testTopicStateManager = manager.GetTopicStateManager("test");
            var testTopicStateManager2 = manager.GetTopicStateManager("test");
        
            // Assert
            testTopicStateManager.Should().NotBeNull();
            testTopicStateManager.Should().Be(testTopicStateManager2);
        }
    
        [Fact]
        public void DeleteTopicState_ShouldReturnExpected()
        {
            // Arrange
            var manager = CreateAppStateManager();
        
            // Act & Assert
            manager.DeleteTopicState("test").Should().BeFalse();
            var testTopicStateManager = manager.GetTopicStateManager("test");
            manager.DeleteTopicState("test").Should().BeTrue();
            var testTopicStateManager2 = manager.GetTopicStateManager("test");
        
            testTopicStateManager2.Should().NotBeNull();
            testTopicStateManager2.Should().NotBe(testTopicStateManager);
        }
    
        [Fact]
        public void DeleteTopicStates_ShouldReturnExpected()
        {
            // Arrange
            var manager = CreateAppStateManager();
            var testTopicStateManager = manager.GetTopicStateManager("test");
            var testTopicStateManager2 = manager.GetTopicStateManager("test2");
        
            // Act 
            var count = manager.DeleteTopicStates();
        
            // Arrange
            count.Should().Be(2);
            manager.GetTopicStates().Should().BeEmpty();
            manager.GetTopicStateManager("test").Should().NotBe(testTopicStateManager);
        }
    
    }
}