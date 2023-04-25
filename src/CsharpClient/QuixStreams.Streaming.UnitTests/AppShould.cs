using System;
using FluentAssertions;
using QuixStreams.State.Storage;
using Xunit;

namespace QuixStreams.Streaming.UnitTests;

public class AppShould
{
    
    [Fact]
    public void GetStatemanager_WithoutSetStateStorage_ShouldNotThrowException()
    {
        // Act
        var manager = App.GetStateManager();
    }
    
    [Fact(Skip = "Until reworked to use non-signleton only one of these tests will pass")]
    public void SetStateStorage_ShouldNotThrowException()
    {
        // Act
        App.SetStateStorage(new InMemoryStateStorage());
    }
    
    [Fact(Skip = "Until reworked to use non-signleton only one of these tests will pass")]
    public void SetStateStorage_CalledTwice_ShouldThrowException()
    {
        // Arrange
        App.SetStateStorage(new InMemoryStateStorage());
        
        // Act
        Action action = () => App.SetStateStorage(new InMemoryStateStorage());
        
        // Assert
        action.Should().Throw<InvalidOperationException>();
    }
}