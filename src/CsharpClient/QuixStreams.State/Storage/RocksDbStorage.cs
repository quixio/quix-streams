using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RocksDbSharp;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// Key/Value storage using RocksDB that implements <see cref="IStateStorage"/> interface.
    /// </summary>
    public class RocksDbStorage : IStateStorage, IDisposable
    {
        private readonly RocksDb db;
        private readonly string dbDirectory;
        private ColumnFamilyHandle columnFamily = null;
        private readonly string storageName;
        
        private readonly WriteBatch writeBatch = new WriteBatch();
        private bool useWriteBatch = false;
        
        private readonly Dictionary<string, RocksDbStorage> subStorages = new Dictionary<string, RocksDbStorage>();

        /// <summary>
        /// Instantiates a new instance of <see cref="RocksDbStorage"/>
        /// </summary>
        /// <param name="dbDirectory">The directory for storing the states</param>
        /// <param name="storageName">Name of the storage</param>
        public RocksDbStorage(string dbDirectory, string storageName)
        {
            if (string.IsNullOrEmpty(dbDirectory) || string.IsNullOrEmpty(storageName))
            {
                throw new ArgumentException("dbDirectory and storageName cannot be null or empty");
            }
            
            if (!Directory.Exists(dbDirectory))
            {
                Directory.CreateDirectory(dbDirectory);
            }

            var dbOptions = new DbOptions().SetCreateIfMissing();
            var columnFamilies = new ColumnFamilies();
            if (RocksDb.TryListColumnFamilies(dbOptions, dbDirectory, out var columnFamiliesNames))
            {
                foreach (var cfName in columnFamiliesNames)
                {
                    columnFamilies.Add(cfName, new ColumnFamilyOptions());
                }
            }
            
            this.db = RocksDb.Open(dbOptions, dbDirectory, columnFamilies);
            
            if (!db.TryGetColumnFamily(storageName, out var storageCf))
            {
                storageCf = db.CreateColumnFamily(new ColumnFamilyOptions(), storageName);
            }

            this.dbDirectory = dbDirectory;
            this.columnFamily = storageCf;
            this.storageName = storageName;
        }
        
        /// <inheritdoc/>
        public Task SaveRaw(string key, byte[] data)
        {
            var byteKey = Encoding.UTF8.GetBytes(key);
            if (useWriteBatch)
            {
                writeBatch.Put(byteKey, data, cf: columnFamily);
            }
            else
            {
                db.Put(byteKey, data, cf: columnFamily);    
            }
            
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<byte[]> LoadRaw(string key)
        {
            var byteKey = Encoding.UTF8.GetBytes(key);
            return Task.FromResult(db.Get(byteKey, cf: columnFamily));
        }

        /// <inheritdoc/>
        public Task RemoveAsync(string key)
        {
            db.Remove(key, cf: columnFamily);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<bool> ContainsKeyAsync(string key)
        {
            var contains = db.Get(key, cf: columnFamily) != null;
            return Task.FromResult(contains);
        }

        /// <inheritdoc/>
        public async Task<string[]> GetAllKeysAsync()
        {
            try
            {
                return await Task.Run(() =>
                {
                    var keys = new List<string>();
                    using (var iterator = db.NewIterator(cf: columnFamily))
                    {
                        iterator.SeekToFirst();
                        while (iterator.Valid())
                        {
                            keys.Add(iterator.StringKey());
                            iterator.Next();
                        }
                    }
                    return keys.ToArray();
                });
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to retrieve all keys", ex);
            }
        }
        
        
        /// <inheritdoc/>
        public Task ClearAsync()
        {
            db.DropColumnFamily(storageName);
            columnFamily = db.CreateColumnFamily(new ColumnFamilyOptions(), storageName);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public async Task<int> Count()
        {
            return (await GetAllKeysAsync()).Length;
        }

        /// <inheritdoc/>
        public bool IsCaseSensitive => true;
        
        /// <inheritdoc/>
        public bool CanPerformTransactions => true;
        
        /// <inheritdoc/>
        public IStateStorage GetOrCreateSubStorage(string subStorageName, string dbName = null)
        {
            if (string.IsNullOrEmpty(dbName))
                dbName = subStorageName;

            if (subStorages.TryGetValue(subStorageName, out var subStorage))
            {
                return subStorage;
            }
            
            subStorage = new RocksDbStorage(GetSubDatabasePath(dbName), subStorageName);
            subStorages.Add(subStorageName, subStorage);

            return subStorage;
        }

        /// <inheritdoc/>
        public bool DeleteSubStorage(string subStorageName, string dbName = null)
        {
            if (string.IsNullOrEmpty(dbName))
                dbName = subStorageName;

            if (!subStorages.TryGetValue(subStorageName, out var subStorage))
            {
                subStorage = new RocksDbStorage(GetSubDatabasePath(dbName), subStorageName);
            }
            
            subStorage.Clear();
            
            if (IsDatabaseEmpty(subStorage))
            {
                Directory.Delete(subStorage.dbDirectory, true);
            }
            
            subStorage.Dispose();
            return true;
        }

        /// <inheritdoc/>
        public int DeleteSubStorages()
        {
            var deleted = 0;
            foreach (var dbDirectory in Directory.EnumerateDirectories(this.dbDirectory))
            {
                Directory.Delete(dbDirectory, true);
                deleted++;
            }
            return deleted;
        }

        /// <inheritdoc/>
        public IEnumerable<string> GetSubStorages()
        {
            var storageNames = new List<string>();
            
            foreach (var dbName in Directory.EnumerateDirectories(this.dbDirectory).Select(Path.GetFileName))
            {
                RocksDb.TryListColumnFamilies(new DbOptions(), GetSubDatabasePath(dbName), out var columnFamiliesNames);
                storageNames.AddRange(columnFamiliesNames ?? Array.Empty<string>());
            }

            return storageNames;
        }

        /// <inheritdoc/>
        public void StartTransaction()
        {
            useWriteBatch = true;
        }
        
        /// <inheritdoc/>
        public bool CommitTransaction()
        {
            try
            {
                db.Write(writeBatch);
                writeBatch.Clear();
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }
        
        /// <summary>
        /// Disposes the underlying RocksDB instance
        /// </summary>
        public void Dispose()
        {
            db.Dispose();
            writeBatch.Dispose();
        }
        
        private string GetSubDatabasePath(string dbName) => $"{this.dbDirectory}{Path.DirectorySeparatorChar}{dbName}";

        private static bool IsDatabaseEmpty(RocksDbStorage storage)
        {
            using var iterator = storage.db.NewIterator();
            iterator.SeekToFirst();
            
            return iterator.Valid();
        }
    }
}
