using System;
using System.IO;
using System.Linq;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.States;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States
{
    public class StreamStateManagerShould
    {
        private readonly StreamStateManager stateManager;
        
        public StreamStateManagerShould()
        {
            // Use a unique name for each test run to avoid conflicting with other tests
            stateManager = StreamStateManager.GetOrCreate(null, new StreamConsumerId(Guid.NewGuid().ToString(), "myTopic", 1, "myStream"), NullLoggerFactory.Instance);
        }
        
        [Fact]
        public void GetDictionaryState_ShouldReturnSameManagerAlways()
        {
            // Arrange
            var manager = this.stateManager;
        
            // Act
            var testStreamState = manager.GetDictionaryState("test");
            var testStreamState2 = manager.GetDictionaryState("test");
        
            // Assert
            testStreamState.Should().NotBeNull();
            testStreamState.Should().BeSameAs(testStreamState2);
        }
    
        [Fact]
        public void GetDictionaryState_StateNameUsedByDifferentStateType_ShouldThrow()
        {
            // Arrange
            var manager = this.stateManager;
            manager.GetDictionaryState("test");
        
            // Assert
            Action scalarStateAct = () => manager.GetScalarState("test");
            scalarStateAct.Should().Throw<ArgumentException>();
        
            Action dictGenericStateAct = () => manager.GetDictionaryState<int>("test");
            dictGenericStateAct.Should().Throw<ArgumentException>();
        
            Action scalarGenericStateAct = () => manager.GetScalarState<int>("test");
            scalarGenericStateAct.Should().Throw<ArgumentException>();
        }
    
        [Fact]
        public void GetDictionaryStateGeneric_StateNameUsedByDifferentStateType_ShouldThrow()
        {
            // Arrange
            var manager = this.stateManager;
            manager.GetDictionaryState<bool>("test");
        
            // Assert
            Action dictStateAct = () => manager.GetDictionaryState("test");
            dictStateAct.Should().Throw<ArgumentException>();

            Action dictDiffGenericStateAct = () => manager.GetDictionaryState<int>("test");
            dictDiffGenericStateAct.Should().Throw<ArgumentException>();
        
            Action scalarStateAct = () => manager.GetScalarState("test");
            scalarStateAct.Should().Throw<ArgumentException>();
        }
    
        [Fact]
        public void GetScalarState_StateNameUsedByDifferentStateType_ShouldThrow()
        {
            // Arrange
            var manager = this.stateManager;
            manager.GetScalarState("test");
        
            // Assert
            Action scalarGenericStateAct = () => manager.GetScalarState<int>("test");
            scalarGenericStateAct.Should().Throw<ArgumentException>();
        
            Action dictStateAct = () => manager.GetDictionaryState("test");
            dictStateAct.Should().Throw<ArgumentException>();
        
            Action dictDiffGenericStateAct = () => manager.GetDictionaryState<int>("test");
            dictDiffGenericStateAct.Should().Throw<ArgumentException>();
        
        }
    
        [Fact]
        public void GetScalarStateGeneric_StateNameUsedByDifferentStateType_ShouldThrow()
        {
            // Arrange
            var manager = this.stateManager;
            manager.GetScalarState<bool>("test");
        
            // Assert
            Action scalarDiffGenericStateAct = () => manager.GetDictionaryState<int>("test");
            scalarDiffGenericStateAct.Should().Throw<ArgumentException>();
        
            Action dictStateAct = () => manager.GetDictionaryState("test");
            dictStateAct.Should().Throw<ArgumentException>();
        
            Action dictGenericStateAct = () => manager.GetDictionaryState<int>("test");
            dictGenericStateAct.Should().Throw<ArgumentException>();
        }
    
        [Fact]
        public void DeleteState_ShouldReturnExpected()
        {
            // Arrange
            var manager = this.stateManager;
        
            // Act & Assert
            manager.DeleteState("test");
            var testStreamState = manager.GetDictionaryState("test");
            manager.DeleteState("test");
            var testStreamState2 = manager.GetDictionaryState("test");
        
            testStreamState2.Should().NotBeNull();
            testStreamState2.Should().NotBeSameAs(testStreamState);
        }
    
        [Fact]
        public void DeleteStates_ShouldReturnExpected()
        {
            // Arrange
            var manager = this.stateManager;
            var testStreamState = manager.GetDictionaryState<string>("testState", key => null);
            
            // Act 
            testStreamState["key"] = "val";
            manager.DeleteStates();
            
            // Assert
            var testStreamStateAfterDelete = manager.GetDictionaryState<string>("testState", key => null);
            testStreamStateAfterDelete.Values.Should().BeEmpty();
            testStreamStateAfterDelete["key"].Should().BeNull();
        }
    }
}