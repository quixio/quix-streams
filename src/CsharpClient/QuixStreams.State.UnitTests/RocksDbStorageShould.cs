using FluentAssertions;
using Xunit;
using QuixStreams.State.Storage;
using System.Threading.Tasks;
using System;
using System.IO;
using RocksDbSharp;

namespace QuixStreams.State.UnitTests
{
    public class RocksDbStorageShould : IDisposable
    {
        private readonly RocksDbStorage storage;
        private readonly string dbDirectory;

        public RocksDbStorageShould()
        {
            // Use a unique name for each test run to avoid conflicting with other tests
            this.dbDirectory = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}");
            storage = RocksDbStorage.GetStateStorage(dbDirectory, "test", "stateName");
        }
        
        [Fact]
        public async Task SaveAndLoadRaw_SavesAndLoadsData()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };

            // Act
            await storage.SaveRaw("key", testData);
            storage.Flush();
            var data = await storage.LoadRaw("key");

            // Assert
            data.Should().BeEquivalentTo(testData);
        }
        
        [Fact]
        public async Task ContainsKeyAsync_ReturnsTrueWhenKeyExists()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };
            await storage.SaveRaw("key", testData);
            await storage.SaveRaw("key2", testData);
            storage.Flush();

            // Act
            var containsKey = await storage.ContainsKeyAsync("key");
            var containsKey2 = await storage.ContainsKeyAsync("key2");

            // Assert
            containsKey.Should().BeTrue();
            containsKey2.Should().BeTrue();
        }
        
        [Fact]
        public async Task ContainsKeyAsync_ReturnsFalseWhenKeyDoesNotExist()
        {
            // Act
            var containsKey = await storage.ContainsKeyAsync("nonExistingKey");

            // Assert
            containsKey.Should().BeFalse();
        }
        
        [Fact]
        public async Task RemoveAsync_RemovesKey()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };
            await storage.SaveRaw("key", testData);

            // Act
            await storage.RemoveAsync("key");
            var containsKey = await storage.ContainsKeyAsync("key");

            // Assert
            containsKey.Should().BeFalse();
        }
        
        [Fact]
        public async Task GetAllKeysAsync_GetsAllKeys()
        {
            // Arrange
            await storage.SaveRaw("key1", new byte[] { 1, 2, 3 });
            await storage.SaveRaw("key2", new byte[] { 4, 5, 6 });
            storage.Flush();
            
            // Act
            var keys = await storage.GetAllKeysAsync();
            
            // Assert
            keys.Should().Contain(new[] { "key1", "key2" });
        }


        [Fact]
        public async Task ClearAsync_RemovesAllData()
        {
            // Arrange
            await storage.SaveRaw("key1", new byte[] { 1, 2, 3 });
            await storage.SaveRaw("key2", new byte[] { 4, 5, 6 });

            // Act
            await storage.ClearAsync();
            var keys = await storage.GetAllKeysAsync();

            // Assert
            keys.Should().BeEmpty();
        }

        [Fact]
        public async Task Count_ReturnsCorrectNumberOfKeys()
        {
            // Arrange
            await storage.SaveRaw("key1", new byte[] { 1, 2, 3 });
            await storage.SaveRaw("key2", new byte[] { 4, 5, 6 });
            storage.Flush();
            
            // Act
            var count = await storage.Count();
            
            // Assert
            count.Should().Be(2);
        }

        [Fact]
        public async Task Transactions_DoesntSaveDataUntilCommitTransactionIsCalled()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };

            // Act
            await storage.SaveRaw("key", testData);
            var loadedData = await storage.LoadRaw("key");

            // Assert
            loadedData.Should().BeNull();

            // Act
            var action = new Action(() => storage.Flush());

            // Assert
            action.Should().NotThrow();
            loadedData = await storage.LoadRaw("key");
            loadedData.Should().BeEquivalentTo(testData);
        }

        [Fact]
        public async Task ContainsKeyAsync_ReturnsTrueIfKeyExists()
        {
            // Arrange
            var key = "existingKey";
            await storage.SaveRaw(key, new byte[] { 1, 2, 3 });
            storage.Flush();
            
            // Act
            var result = await storage.ContainsKeyAsync(key);

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task ContainsKeyAsync_ReturnsFalseIfKeyDoesNotExist()
        {
            // Arrange
            var key = "nonExistingKey";

            // Act
            var result = await storage.ContainsKeyAsync(key);

            // Assert
            result.Should().BeFalse();
        }

        [Fact]
        public async Task RemoveAsync_RemovesExistingKey()
        {
            // Arrange
            var key = "keyToRemove";
            await storage.SaveRaw(key, new byte[] { 1, 2, 3 });

            // Act
            await storage.RemoveAsync(key);
            var result = await storage.ContainsKeyAsync(key);

            // Assert
            result.Should().BeFalse();
        }

        [Fact]
        public void RemoveAsync_DoesNotThrowWhenKeyDoesNotExist()
        {
            // Arrange
            var key = "nonExistingKey";

            // Act
            Func<Task> action = async () => await storage.RemoveAsync(key);

            // Assert
            action.Should().NotThrow();
        }
        
        [Fact]
        public void Constructor_ThrowsException_WhenDbDirectoryIsNull()
        {
            // Act
            Func<RocksDbStorage> act = () => RocksDbStorage.GetStateStorage(null, "test", "stateName");

            // Assert
            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Constructor_ThrowsException_WhenStorageNameIsNullOrDefault()
        {
            // Act
            Func<RocksDbStorage> act = () => RocksDbStorage.GetStateStorage("testDir", null, "stateName");

            // Assert
            act.Should().Throw<ArgumentException>();
        }
        
        [Fact]
        public void Dipose_SecondProcessesShouldBeAbleToAccessTheDbAfterDisposed()
        { 
            // Act
            var openRocksDBinSecondProcessTask = Task.Run(() => AttemptToOpenRocksDb(dbDirectory));

            // Assert
            openRocksDBinSecondProcessTask.Result.Should().BeFalse("because the second process shouldn't be able to open a RocksDB connection, as one is already open at the same location.");
            
            // Act
            storage.Dispose();
            openRocksDBinSecondProcessTask = Task.Run(() => AttemptToOpenRocksDb(dbDirectory));
            
            // Assert
            openRocksDBinSecondProcessTask.Result.Should().BeTrue("because the second process should be able to open a RocksDB connection, as the connection of the first db was disposed.");
        }
        
        /// <summary>
        /// Attempts to open a RocksDB instance. Returns true if successful, and false if an exception with "lock" keyword is encountered.
        /// </summary>
        private bool AttemptToOpenRocksDb(string dbPath)
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
        
        public void Dispose()
        {
            // Cleanup
            storage.Dispose();
            System.IO.Directory.Delete(this.dbDirectory, true);
        }
    }
}