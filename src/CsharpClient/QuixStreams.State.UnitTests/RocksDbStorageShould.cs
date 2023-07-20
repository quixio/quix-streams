using FluentAssertions;
using Xunit;
using QuixStreams.State.Storage;
using System.Threading.Tasks;
using System;
using System.IO;

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
            storage = new RocksDbStorage(dbDirectory, "test");
        }

        [Fact]
        public async Task SaveAndLoadRaw_WithSameDbNameButDifferentStorages_SeparatesDataForEachStorage()
        {
            // Arrange
            var testData1 = new byte[] { 1, 1, 1 };
            var testData2 = new byte[] { 1, 2, 2 };
            var subStorage1 = storage.GetOrCreateSubStorage("subStorage1");
            var subStorage2 = storage.GetOrCreateSubStorage("subStorage2");

            // Act
            await subStorage1.SaveRaw("key", testData1);
            await subStorage2.SaveRaw("key", testData2);

            // Assert
            var data1 = await subStorage1.LoadRaw("key");
            var data2 = await subStorage2.LoadRaw("key");

            data1.Should().BeEquivalentTo(testData1);
            data2.Should().BeEquivalentTo(testData2);
        }

        [Fact]
        public async Task SaveAndLoadRaw_SavesAndLoadsData()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };

            // Act
            await storage.SaveRaw("key", testData);
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

            // Act
            var containsKey = await storage.ContainsKeyAsync("key");

            // Assert
            containsKey.Should().BeTrue();
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
            var keys = await storage.GetAllKeysAsync();
            await storage.ClearAsync();
            keys = await storage.GetAllKeysAsync();

            // Assert
            keys.Should().BeEmpty();
        }

        [Fact]
        public async Task Count_ReturnsCorrectNumberOfKeys()
        {
            // Arrange
            await storage.SaveRaw("key1", new byte[] { 1, 2, 3 });
            await storage.SaveRaw("key2", new byte[] { 4, 5, 6 });

            // Act
            var count = await storage.Count();

            // Assert
            count.Should().Be(2);
        }

        [Fact]
        public async Task GetOrCreateSubStorage_CreatesNewSubStorage()
        {
            // Act
            var subStorage = storage.GetOrCreateSubStorage("subStorage1");

            // Assert
            subStorage.Should().NotBeNull();
        }
        
        [Fact]
        public async Task GetOrCreateSubStorage_GetsTheExistingIfExists()
        {
            // Act
            storage.GetOrCreateSubStorage("subStorage1");
            storage.GetOrCreateSubStorage("subStorage1");

            // Act
            var subStorages = storage.GetSubStorages();
            
            // Assert
            subStorages.Should().HaveCount(2);
        }

        [Fact]
        public async Task DeleteSubStorage_DeletesExistingSubStorage()
        {
            // Arrange
            var subStorageName = "subStorage2";
            storage.GetOrCreateSubStorage(subStorageName);

            // Act
            var result = storage.DeleteSubStorage(subStorageName);

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task DeleteSubStorages_DeletesAllSubStorages()
        {
            // Arrange
            storage.GetOrCreateSubStorage("subStorage3");
            storage.GetOrCreateSubStorage("subStorage4");

            // Act
            var deletedCount = storage.DeleteSubStorages();

            // Assert
            deletedCount.Should().Be(2);
        }

        [Fact]
        public async Task GetSubStorages_ReturnsAllSubStorages()
        {
            // Arrange
            var subStorage1 = "subStorage5";
            var subStorage2 = "subStorage6";
            storage.GetOrCreateSubStorage(subStorage1);
            storage.GetOrCreateSubStorage(subStorage2);

            // Act
            var storages = storage.GetSubStorages();

            // Assert
            storages.Should().Contain(new[] { subStorage1, subStorage2 });
        }

        [Fact]
        public async Task Transactions_DoesntSaveDataUntilCommitTransactionIsCalled()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };

            // Act
            storage.StartTransaction();

            await storage.SaveRaw("key", testData);
            var loadedData = await storage.LoadRaw("key");

            // Assert
            loadedData.Should().BeNull();

            // Act
            var result = storage.CommitTransaction();

            // Assert
            result.Should().BeTrue();
            loadedData = await storage.LoadRaw("key");
            loadedData.Should().BeEquivalentTo(testData);
        }

        [Fact]
        public async Task ContainsKeyAsync_ReturnsTrueIfKeyExists()
        {
            // Arrange
            var key = "existingKey";
            await storage.SaveRaw(key, new byte[] { 1, 2, 3 });

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
            Func<RocksDbStorage> act = () => new RocksDbStorage(null, "test");

            // Assert
            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Constructor_ThrowsException_WhenStorageNameIsNull()
        {
            // Act
            Func<RocksDbStorage> act = () => new RocksDbStorage("testDir", null);

            // Assert
            act.Should().Throw<ArgumentException>();
        }

        public void Dispose()
        {
            // Cleanup
            storage.Dispose();
            System.IO.Directory.Delete(this.dbDirectory, true);
        }
    }
}