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
    
    [Fact]
    public void SetStateStorage_ShouldNotThrowException()
    {
        // Act
        App.SetStateStorage(new InMemoryStateStorage());
    }
    
    [Fact]
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