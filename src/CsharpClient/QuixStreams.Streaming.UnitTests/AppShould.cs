using System;
using FluentAssertions;
using QuixStreams.State.Storage;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{
    public class AppShould
    {
    
        [Fact]
        public void GetStateManager_WithoutSetStateStorage_ShouldNotThrowException()
        {
            // Act
            var manager = App.GetStateManager();
        }
    
        [Fact(Skip = "Until reworked to use non-singleton only one of these tests will pass")]
        public void SetStateStorage_ShouldNotThrowException()
        {
            // Act
            App.SetStateStorage(new InMemoryStorage());
        }
    
        [Fact(Skip = "Until reworked to use non-singleton only one of these tests will pass")]
        public void SetStateStorage_CalledTwice_ShouldThrowException()
        {
            // Arrange
            App.SetStateStorage(new InMemoryStorage());
        
            // Act
            Action action = () => App.SetStateStorage(new InMemoryStorage());
        
            // Assert
            action.Should().Throw<InvalidOperationException>();
        }
    }
}