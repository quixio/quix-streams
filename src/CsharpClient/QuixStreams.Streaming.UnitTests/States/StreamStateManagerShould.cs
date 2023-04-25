using System.Linq;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.States;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States;

public class StreamStateManagerShould
{
    private StreamStateManager CreateStreamStateManager()
    {
        return new StreamStateManager("myStream", new InMemoryStateStorage(), NullLoggerFactory.Instance, "TEST - ");
    }
    
    [Fact]
    public void GetStates_ShouldReturnExcepted()
    {
        // Arrange
        var manager = CreateStreamStateManager();

        // Assert
        var states = manager.GetStates().ToArray();
        states.Length.Should().Be(0);
        
        // Act
        manager.GetState("test");
        manager.GetState("test2");
        manager.GetState("test2"); // get it twice
        manager.GetState("test3");
        manager.DeleteState("test3");
        
        // Assert
        states = manager.GetStates().ToArray();
        states.Should().BeEquivalentTo(new[] { "test", "test2" });
    }
    
    [Fact]
    public void GetState_ShouldReturnSameManagerAlways()
    {
        // Arrange
        var manager = CreateStreamStateManager();
        
        // Act
        var testStreamState = manager.GetState("test");
        var testStreamState2 = manager.GetState("test");
        
        // Assert
        testStreamState.Should().NotBeNull();
        testStreamState.Should().BeSameAs(testStreamState2);
    }
    
    [Fact]
    public void DeleteState_ShouldReturnExpected()
    {
        // Arrange
        var manager = CreateStreamStateManager();
        
        // Act & Assert
        manager.DeleteState("test").Should().BeFalse();
        var testStreamState = manager.GetState("test");
        manager.DeleteState("test").Should().BeTrue();
        var testStreamState2 = manager.GetState("test");
        
        testStreamState2.Should().NotBeNull();
        testStreamState2.Should().NotBeSameAs(testStreamState);
    }
    
    [Fact]
    public void DeleteStates_ShouldReturnExpected()
    {
        // Arrange
        var manager = CreateStreamStateManager();
        var testStreamState = manager.GetState("test");
        var testStreamState2 = manager.GetState("test2");
        
        // Act 
        var count = manager.DeleteStates();
        
        // Arrange
        count.Should().Be(2);
        manager.GetStates().Should().BeEmpty();
        manager.GetState("test").Should().NotBeSameAs(testStreamState);
    }
}