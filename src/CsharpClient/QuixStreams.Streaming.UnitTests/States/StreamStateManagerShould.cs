using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.States;
using RocksDbSharp;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States
{
    public class StreamStateManagerShould
    {
        private readonly StreamStateManager stateManager;
        private readonly StreamConsumerId streamConsumerId;
        
        public StreamStateManagerShould()
        {
            try { App.SetStateStorageType(StateStorageTypes.RocksDb); }
            catch (InvalidOperationException ex)
            {
                if (!ex.Message.Contains("may only be set once"))
                {
                    throw;
                }
                //else ignore
            }

            // Use a unique name for each test run to avoid conflicting with other tests
            streamConsumerId = new StreamConsumerId(Guid.NewGuid().ToString(), "myTopic", 1, "myStream");
            stateManager = StreamStateManager.GetOrCreate(null, streamConsumerId, NullLoggerFactory.Instance);
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
        
        [Fact]
        public void Revoke_ShouldDisposeTheStorage()
        {
            // Act
            this.stateManager.GetDictionaryState<string>("testState", key => null);
            var openRocksDBinSecondProcessTask = Task.Run(() => AttemptToOpenRocksDb(this.stateManager.StorageDir));

            // Assert
            openRocksDBinSecondProcessTask.Result.Should().BeFalse("because the second process shouldn't be able to open a RocksDB connection, as one is already open at the same location.");
            
            // Act
            StreamStateManager.TryRevoke(streamConsumerId);
            openRocksDBinSecondProcessTask = Task.Run(() => AttemptToOpenRocksDb(this.stateManager.StorageDir));
            
            // Assert
            openRocksDBinSecondProcessTask.Result.Should().BeTrue("because the second process should be able to open a RocksDB connection, as the connection of the first db was disposed.");
        }
        
        /// <summary>
        /// Attempts to open a RocksDB instance. Returns true if successful, and false if an exception with "lock" keyword is encountered.
        /// </summary>
        public bool AttemptToOpenRocksDb(string dbPath)
        {
            try
            {
                using (RocksDb.Open(new DbOptions(), dbPath))
                {
                    // This code path means that it was able to access the DB
                    return true;
                }
            }
            catch (RocksDbException ex)
            {
                // Returns false if the exception message contains the keyword "lock".
                return !ex.Message.Contains("lock");
            }
        }
    }
}