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
            storage = new RocksDbStorage(dbDirectory, "test");
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
        public async Task SaveAndLoadRaw_WithSubStoragesSharingDb_SeparatesDataOfSubStorages()
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
        public async Task ContainsAsync_WithSubStoragesSharingDb_ReturnsIfKeyExistsFromCorrectSubStorage()
        {
            // Arrange
            var subStorage1 = storage.GetOrCreateSubStorage("subStorage1");
            var subStorage2 = storage.GetOrCreateSubStorage("subStorage2");

            // Act
            await subStorage1.SaveRaw("key", new byte[] { 1, 1, 1 });
            var containsResult1 = await subStorage1.ContainsKeyAsync("key");
            var containsResult2 = await subStorage2.ContainsKeyAsync("key");

            // Assert
            containsResult1.Should().BeTrue();
            containsResult2.Should().BeFalse();
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
        public async Task RemoveAsync_WithSubStoragesSharingDb_RemovesKeyFromCorrectSubStorage()
        {
            // Arrange
            var testData = new byte[] { 1, 2, 3 };
            var subStorage1 = storage.GetOrCreateSubStorage("subStorage1");
            var subStorage2 = storage.GetOrCreateSubStorage("subStorage2");
            var subStorage3 = storage.GetOrCreateSubStorage("subStorage3");

            // Act
            await subStorage1.SaveRaw("key", testData);
            await subStorage2.SaveRaw("key", testData);
            await subStorage3.SaveRaw("key", testData);
            
            await subStorage2.RemoveAsync("key");
            

            // Assert
            var data1 = await subStorage1.LoadRaw("key");
            var data2 = await subStorage2.LoadRaw("key");
            var data3 = await subStorage3.LoadRaw("key");

            data1.Should().BeEquivalentTo(testData);
            data2.Should().BeNull();
            data3.Should().BeEquivalentTo(testData);
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
        public async Task GetAllKeysAsync_WithSubStoragesSharingDb_GetsAllKeysFromCorrectSubStorage()
        {
            // Arrange
            var subStorage1 = storage.GetOrCreateSubStorage("subStorage1");
            var subStorage2 = storage.GetOrCreateSubStorage("subStorage2");
            
            // Act
            await subStorage1.SaveRaw("key1", new byte[] { 1, 2, 3 });
            await subStorage2.SaveRaw("key1", new byte[] { 4, 5, 6 });
            await subStorage2.SaveRaw("key2", new byte[] { 7, 8, 9 });

            var keys = await subStorage2.GetAllKeysAsync();
            
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
        public async Task ClearAsync_WithSubStoragesSharingDb_RemovesAllDataForCorrectSubStorage()
        {
            // Arrange
            var subStorage1 = storage.GetOrCreateSubStorage("subStorage1");
            var subStorage2 = storage.GetOrCreateSubStorage("subStorage2");
            
            // Act
            await subStorage1.SaveRaw("key1", new byte[] { 1, 2, 3 });
            await subStorage2.SaveRaw("key2", new byte[] { 4, 5, 6 });
            
            await subStorage1.ClearAsync();
            var keys = await subStorage1.GetAllKeysAsync();
            
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
        public void GetOrCreateSubStorage_CreatesNewSubStorage()
        {
            // Act
            var subStorage = storage.GetOrCreateSubStorage("subStorage1");

            // Assert
            subStorage.Should().NotBeNull();
        }
        
        [Fact]
        public void GetOrCreateSubStorage_DoesntCreateNewIfAlreadyExists()
        {
            // Act
            storage.GetOrCreateSubStorage("subStorage1");
            storage.GetOrCreateSubStorage("subStorage2");
            storage.GetOrCreateSubStorage("subStorage1");

            // Assert
            storage.GetSubStorages().Should().HaveCount(2);
        }
        
        [Fact]
        public void GetOrCreateSubStorage_ThrowsExceptionWhenCreatingSubStorageWithSameNameButDifferentDb()
        {
            // Arrange
            storage.GetOrCreateSubStorage("subStorage1");

            // Act
            Func<IStateStorage> act = () => storage.GetOrCreateSubStorage("subStorage1", "newDb");

            // Assert
            act.Should().Throw<ArgumentException>();
        }
        
        [Fact]
        public void GetOrCreateSubStorage_CreatesNewSubStorageWhenAllDeleted()
        {
            // Arrange
            var sub1 = storage.GetOrCreateSubStorage("subStorage1");
            var sub2 = sub1.GetOrCreateSubStorage("subStorage2", "newDb-depth1");
            sub2.GetOrCreateSubStorage("subStorage3", "newDb-depth2");
                        
            // Deleting and re-creating them
            storage.DeleteSubStorages();
            storage.GetSubStorages().Should().BeEmpty();
            
            sub1 = storage.GetOrCreateSubStorage("subStorage1");
            sub2 = sub1.GetOrCreateSubStorage("subStorage2", "newDb-depth1");
            sub2.GetOrCreateSubStorage("subStorage3", "newDb-depth2");
            
            // Assert
            storage.GetSubStorages().Should().HaveCount(1);
            sub1.GetSubStorages().Should().HaveCount(1);
            sub2.GetSubStorages().Should().HaveCount(1);
        }
        
        [Fact]
        public void GetOrCreateSubStorage_CreatesNewIfPreviouslyDeleted()
        {
            // Arrange
            storage.GetOrCreateSubStorage("subStorage1");
            storage.GetOrCreateSubStorage("subStorage2", "newDb");
            storage.GetOrCreateSubStorage("subStorage3", "anotherDb");

            // Act
            storage.DeleteSubStorage("subStorage1");
            storage.DeleteSubStorage("subStorage2");
            storage.GetSubStorages().Should().ContainSingle();
            
            storage.GetOrCreateSubStorage("subStorage1");
            storage.GetOrCreateSubStorage("subStorage2", "newDb");
            
            // Assert
            storage.GetSubStorages().Should().HaveCount(3);
        }

        [Fact]
        public void DeleteSubStorage_DeletesExistingSubStorage()
        {
            // Arrange
            var sub1 = storage.GetOrCreateSubStorage("subStorage1");
            sub1.GetOrCreateSubStorage("subStorage2");

            // Act
            var result = storage.DeleteSubStorage("subStorage1");

            // Assert
            result.Should().BeTrue();
            storage.GetSubStorages().Should().BeEmpty();
        }

        [Fact]
        public void DeleteSubStorages_DeletesAllSubStorages()
        {
            // Arrange
            var sub1 = storage.GetOrCreateSubStorage("subStorage1");
            var sub2 = storage.GetOrCreateSubStorage("subStorage2");
            sub1.GetOrCreateSubStorage("subStorage3");
            sub2.GetOrCreateSubStorage("subStorage4", "newDb");

            // Act
            var deletedCount = storage.DeleteSubStorages();

            // Assert
            deletedCount.Should().Be(4);
        }

        [Fact]
        public void GetSubStorages_ReturnsAllSubStorages()
        {
            // Arrange
            var sub1 = storage.GetOrCreateSubStorage("subStorage1");
            var sub2 = storage.GetOrCreateSubStorage("subStorage2");
            sub1.GetOrCreateSubStorage("subStorage3");
            sub2.GetOrCreateSubStorage("subStorage4", "newDb");

            // Act
            var storages = storage.GetSubStorages();

            // Assert
            storages.Should().Contain(new[] { "subStorage1", "subStorage2" });
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
        public void Constructor_ThrowsException_WhenStorageNameIsNullOrDefault()
        {
            // Act
            Func<RocksDbStorage> act = () => new RocksDbStorage("testDir", null);
            Func<RocksDbStorage> act2 = () => new RocksDbStorage("testDir", ColumnFamilies.DefaultName);

            // Assert
            act.Should().Throw<ArgumentException>();
            act2.Should().Throw<ArgumentException>();
        }

        public void Dispose()
        {
            // Cleanup
            storage.Dispose();
            System.IO.Directory.Delete(this.dbDirectory, true);
        }
        
        [Fact]
        public void OpenTest()
        {
            var columnFamilies2 = new ColumnFamilies();
            columnFamilies2.Add("cfName", new ColumnFamilyOptions());
            columnFamilies2.Add("harisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharisharis", new ColumnFamilyOptions());

            var x = RocksDb.Open(new DbOptions().SetCreateIfMissing().SetCreateMissingColumnFamilies(), "./", columnFamilies2);
            //var x3 = RocksDb.Open(new DbOptions().SetCreateIfMissing(), "./");
        }
    }
}