using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RocksDbSharp;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// Key/Value storage using RocksDB that implements <see cref="IStateStorage"/> interface.
    /// </summary>
    public class RocksDbStorage : IStateStorage, IDisposable
    {
        /// <summary>
        /// Holds references of RocksDb instances to ensure only one exists and avoid IO lock issue.
        /// Key is the db directory and value is a tuple of RocksDb instance and reference count.
        /// </summary>
        private static readonly Dictionary<string, RocksDbInstance> RocksDbInstances = new ();
        
        private readonly RocksDb db;
        private readonly string dbDirectory;
        private ColumnFamilyHandle columnFamily;
        private readonly string storageName;

        private readonly WriteBatch writeBatch = new();
        private bool useWriteBatch = false;

        private const char SubStorageSeparator = '/';
        
        /// <summary>
        /// In-memory loaded sub-storages.   
        /// Key is the sub-storage name and value is RocksDbStorage instance.
        /// </summary>
        private readonly Dictionary<string, RocksDbStorage> subStorages = new();


        /// <summary>
        /// Instantiates a new instance of <see cref="RocksDbStorage"/>
        /// </summary>
        /// <param name="dbDirectory">The directory for storing the states</param>
        /// <param name="storageName">Name of the storage. Used to separate data if other storages use the same database</param>
        public RocksDbStorage(string dbDirectory, string storageName = "default-storage")
        {
            if (string.IsNullOrEmpty(dbDirectory) || string.IsNullOrEmpty(storageName))
            {
                throw new ArgumentException($"{nameof(dbDirectory)} and {nameof(storageName)} cannot be null or empty.");
            }
            
            if (storageName == ColumnFamilies.DefaultName)
            {
                throw new ArgumentException($"{nameof(storageName)} cannot be '{ColumnFamilies.DefaultName}' as it is reserved by rocksdb.");
            }

            this.db = GetOrCreateRocksDbInstance(dbDirectory);
            
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
            if (subStorageName.Contains(SubStorageSeparator))
            {
                throw new ArgumentException($"{nameof(storageName)} cannot contain slash character '{SubStorageSeparator}'.");
            }
            
            subStorageName = $"{this.storageName}{SubStorageSeparator}{subStorageName}";
            var dbDir = string.IsNullOrEmpty(dbName) ? this.dbDirectory : GetSubDatabasePath(dbName);
            
            if (GetSubStoragesWithDb().Any(x => x.Key == subStorageName && x.Value != dbDir))
            {
                var nameWithoutParent = subStorageName.Substring(this.storageName.Length + 1);
                throw new ArgumentException($"Sub-storage with name '{nameWithoutParent}' already exists in a different database");
            }
            
            if (subStorages.TryGetValue(subStorageName, out var subStorage))
            {
                return subStorage;
            }

            subStorage = new RocksDbStorage(dbDir, subStorageName);
            subStorages.Add(subStorageName, subStorage);

            return subStorage;
        }

        /// <inheritdoc/>
        public bool DeleteSubStorage(string subStorageName)
        {
            subStorageName = $"{this.storageName}{SubStorageSeparator}{subStorageName}";

            var subs = GetSubStoragesWithDb();
            
            if (!subs.TryGetValue(subStorageName, out var subStorageDbDirectory))
            {
                return false;
            }
            
            var subStoragesOfTheSubStorage = subs.Where(sub => sub.Key.StartsWith($"{subStorageName}{SubStorageSeparator}"));

            foreach (var sub in subStoragesOfTheSubStorage.Append(new (subStorageName, subStorageDbDirectory)))
            {
                var subDb = GetOrCreateRocksDbInstance(sub.Value);
                
                subDb.DropColumnFamily(sub.Key);

                if (IsDatabaseEmpty(subDb))
                {
                    Directory.Delete(sub.Value, false);
                }
            }
            
            subStorages[subStorageName]?.Dispose();
            subStorages.Remove(subStorageName);
            return true;
        }

        /// <inheritdoc/>
        public int DeleteSubStorages()
        {
            var deleted = 0;
            foreach (var sub in GetSubStoragesWithDb())
            {
                var subDb = GetOrCreateRocksDbInstance(sub.Value);
                
                subDb.DropColumnFamily(sub.Key);
                deleted++;
                
                if (IsDatabaseEmpty(subDb))
                {
                    Directory.Delete(sub.Value, false);
                }
            }

            // Dispose of sub-storages that are referenced in memory
            foreach (var subStorage in subStorages.Values)
            {
                subStorage.Dispose();
            }
            subStorages.Clear();
            return deleted;
        }

        /// <inheritdoc/>
        public IEnumerable<string> GetSubStorages()
        {
            var storageNames = new List<string>();
            
            // Get all sub-storage names
            storageNames.AddRange(GetSubStoragesWithDb().Keys);
            
            // Remove the parent's storage name
            storageNames = storageNames.Select(x => x.Substring(this.storageName.Length + 1)).ToList();
            
            // Remove sub-sub-storages names
            return storageNames.Select(x =>
            {
                var endOfStorageNameIndex = x.IndexOf(SubStorageSeparator);
                return endOfStorageNameIndex == -1 ? x : x.Remove(endOfStorageNameIndex);
            }).ToImmutableHashSet();
        }
        
        private Dictionary<string, string> GetSubStoragesWithDb()
        {
            var subs = new Dictionary<string, string>();

            foreach (var subDbDirectory in Directory.EnumerateDirectories(this.dbDirectory).Append(this.dbDirectory))
            {
                RocksDb.TryListColumnFamilies(new DbOptions(), subDbDirectory, out var columnFamiliesNames);
                foreach (var cfName in columnFamiliesNames)
                {
                    if (cfName == ColumnFamilies.DefaultName)
                        continue;

                    if (!cfName.StartsWith($"{this.storageName}{SubStorageSeparator}"))
                        continue;

                    subs.Add(cfName, subDbDirectory);
                }
            }

            return subs;
        }

        /// <inheritdoc/>
        public void StartTransaction()
        {
            useWriteBatch = true;
        }
        
        /// <inheritdoc/>
        public void CommitTransaction()
        {
            try
            {
                db.Write(writeBatch);
            }
            finally
            {
                writeBatch.Clear();
                useWriteBatch = false;
            }
        }
        
        /// <summary>
        /// Disposes the underlying RocksDB instance
        /// </summary>
        public void Dispose()
        {
            this.writeBatch.Dispose();
            
            if (!RocksDbInstances.TryGetValue(this.dbDirectory, out var dbWithRefCount))
            {
                return;
            }
            
            dbWithRefCount.DecrementRefCount();

            if (!dbWithRefCount.IsUsed())
            {
                dbWithRefCount.Db.Dispose(); // dispose the db
                RocksDbInstances.Remove(dbDirectory);   
            }
        }
        
        private string GetSubDatabasePath(string dbName) => $"{this.dbDirectory}{Path.DirectorySeparatorChar}{dbName}";
        
        private static bool IsDatabaseEmpty(RocksDb db)
        {
            using var iterator = db.NewIterator();
            iterator.SeekToFirst();
            
            return iterator.Valid();
        }

        private static RocksDb GetOrCreateRocksDbInstance(string dbDirectory)
        {
            if (RocksDbInstances.TryGetValue(dbDirectory, out var dbWithRefCount))
            {
                dbWithRefCount.IncrementRefCount();
                return dbWithRefCount.Db;
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
            
            var db = RocksDb.Open(dbOptions, dbDirectory, columnFamilies);
            RocksDbInstances.Add(dbDirectory, new RocksDbInstance(db, 1));

            return db;
        }

        private class RocksDbInstance
        {
            public RocksDb Db { get; }

            private int refCount;

            public RocksDbInstance(RocksDb db, int initialRefCount)
            {
                Db = db;
                refCount = initialRefCount;
            }

            public void IncrementRefCount()
            {
                Interlocked.Increment(ref refCount);
            }
            
            public void DecrementRefCount()
            {
                var result = Interlocked.Decrement(ref refCount);
                if (result < 0)
                {
                    throw new InvalidOperationException("De-referenced the RocksDB instance more times than tracked");
                }
            }

            public bool IsUsed()
            {
                return this.refCount != 0;
            }
        }
    }
}
