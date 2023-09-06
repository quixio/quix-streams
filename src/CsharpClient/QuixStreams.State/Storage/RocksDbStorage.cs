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
        private static readonly string subStoragesCFName = "sub-storages";
        private ColumnFamilyHandle subStoragesCFHandle;

        private readonly RocksDb db;
        private readonly string dbDirectory;
        private readonly string keyPrefix;
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
            
            this.db = GetOrCreateRocksDbInstance(dbDirectory);

            this.subStoragesCFHandle = db.GetColumnFamily(subStoragesCFName);
            this.dbDirectory = dbDirectory;
            this.keyPrefix = $"{storageName}~";
            this.storageName = storageName;
        }
        
        /// <inheritdoc/>
        public Task SaveRaw(string key, byte[] data)
        {
            var byteKey = Encoding.UTF8.GetBytes($"{keyPrefix}{key}");
            if (useWriteBatch)
            {
                writeBatch.Put(byteKey, data);
            }
            else
            {
                db.Put(byteKey, data);    
            }
            
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<byte[]> LoadRaw(string key)
        {
            var byteKey = Encoding.UTF8.GetBytes($"{keyPrefix}{key}");
            return Task.FromResult(db.Get(byteKey));
        }

        /// <inheritdoc/>
        public Task RemoveAsync(string key)
        {
            var byteKey = Encoding.UTF8.GetBytes($"{keyPrefix}{key}");
            db.Remove(byteKey);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<bool> ContainsKeyAsync(string key)
        {
            var keyWithPrefix = $"{keyPrefix}{key}";
            var contains = db.HasKey(keyWithPrefix);
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
                    using (var iterator = db.NewIterator())
                    {
                        iterator.Seek(this.keyPrefix);
                        while (iterator.Valid() && iterator.StringKey().StartsWith(this.keyPrefix))
                        {
                            keys.Add(iterator.StringKey());
                            iterator.Next();
                        }
                    }
                    var keysWithoutPrefix = keys.Select(x => x.Substring(this.keyPrefix.Length)).ToArray();
                    return keysWithoutPrefix;
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
            try
            {
                DeleteKeysWithPrefix(this.keyPrefix);
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to clear the storage", ex);
            }
            return Task.CompletedTask;
        }

        private void DeleteKeysWithPrefix(string prefix, string cf = null)
        {
            var cfHandle = string.IsNullOrEmpty(cf) ? db.GetDefaultColumnFamily() : db.GetColumnFamily(cf);
            var startKey = Encoding.UTF8.GetBytes(prefix);
                
            // To construct an end key, we are taking advantage of the lexicographic ordering.
            // Increment the last character of the prefix to form an end key.
            var endKey = (byte[])startKey.Clone();
            endKey[endKey.Length - 1]++;

            using var batch = new WriteBatch();
            
            batch.DeleteRange(startKey, (ulong)startKey.Length, endKey, (ulong)endKey.Length, cf: cfHandle);
            db.Write(batch);
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
                throw new ArgumentException($"{nameof(storageName)} cannot contain the slash character '{SubStorageSeparator}'.");
            }
            
            subStorageName = $"{this.storageName}{SubStorageSeparator}{subStorageName}";
            var subDbDir = string.IsNullOrEmpty(dbName) ? this.dbDirectory : GetSubDatabasePath(dbName);
            
            // if (GetSubStoragesWithDb().Any(x => x.Key == subStorageName && x.Value != dbDir))
            // {
            //     var nameWithoutParentPrefix = subStorageName.Substring(this.storageName.Length + 1);
            //     throw new ArgumentException($"Sub-storage with name '{nameWithoutParentPrefix}' already exists in a different database");
            // }
            
            if (subStorages.TryGetValue(subStorageName, out var subStorage))
            {
                return subStorage;
            }
            
            subStorage = new RocksDbStorage(subDbDir, subStorageName);
            subStorages.Add(subStorageName, subStorage);
            
            this.db.Put(subStorageName, RocksDbStorageMeta.From(subStorage).ToString(), cf: subStoragesCFHandle);

            return subStorage;
        }

        /// <inheritdoc/>
        public bool DeleteSubStorage(string subStorageName)
        {
            subStorageName = $"{this.storageName}{SubStorageSeparator}{subStorageName}";

            var subs = GetSubStoragesRecursively();
            
            if (!subs.TryGetValue(subStorageName, out var subStorageMeta))
            {
                return false;
            }
            
            var subsToDelete = subs.Where(x => x.Key.StartsWith($"{subStorageName}{SubStorageSeparator}")).Select(x => x.Value).Append(subStorageMeta).ToList();
            
            foreach (var sub in subsToDelete)
            {
                var subDb = GetOrCreateRocksDbInstance(sub.DbDirectory);
                
                // Deleting records that belong to this sub-storage
                DeleteKeysWithPrefix(sub.KeyPrefix);
                
                // Delete the record in sub-storages column family belonging to this sub-storage
                db.Remove(sub.StorageName, cf: db.GetColumnFamily(subStoragesCFName));
                
                // Delete records in sub-storages column family of sub-sub-storages
                DeleteKeysWithPrefix($"{sub.StorageName}{SubStorageSeparator}", cf: subStoragesCFName);
                
                if (IsDatabaseEmpty(subDb))
                {
                    Directory.Delete(sub.DbDirectory, false);
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
            var allSubStorages = GetSubStoragesRecursively();
            var subStoragesOfDepth1 = allSubStorages.Where(x => !x.Key.Substring(this.storageName.Length + 1).Contains(SubStorageSeparator));

            db.DropColumnFamily(subStoragesCFName);
            subStoragesCFHandle = db.CreateColumnFamily(new ColumnFamilyOptions(), subStoragesCFName);
            
            foreach (var sub in subStoragesOfDepth1)
            {
                this.db.Remove(sub.Key, cf: subStoragesCFHandle);
                deleted++;

                if (sub.Value.DbDirectory == this.dbDirectory)
                {
                    DeleteKeysWithPrefix(sub.Value.KeyPrefix);
                }
            }

            foreach (var sub in allSubStorages)
            {
                if (sub.Value.DbDirectory != dbDirectory && Directory.Exists(sub.Value.DbDirectory))
                {
                    Directory.Delete(sub.Value.DbDirectory, true);
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
            // Get all sub-storage names
            var storageNames = GetSubStoragesRecursively().Keys.ToList();
            
            // Remove the parent's storage name
            storageNames = storageNames.Select(x => x.Substring(this.storageName.Length + 1)).ToList();
            
            // Remove sub-sub-storage names
            return storageNames.Select(x =>
            {
                var endOfStorageNameIndex = x.IndexOf(SubStorageSeparator);
                return endOfStorageNameIndex == -1 ? x : x.Remove(endOfStorageNameIndex);
            }).ToImmutableHashSet();
        }
        
        private Dictionary<string, RocksDbStorageMeta> GetSubStoragesRecursively()
        {
            IEnumerable<RocksDbStorageMeta> GetSubStorages(string dbDirectory, string storageName)
            {
                var db = GetOrCreateRocksDbInstance(dbDirectory);
                var subs = new List<RocksDbStorageMeta>();
                
                using var iterator = db.NewIterator(cf: db.GetColumnFamily(subStoragesCFName));
                iterator.SeekToFirst();
                
                while (iterator.Valid())
                {
                    if (iterator.StringKey().StartsWith($"{storageName}{SubStorageSeparator}"))
                    {
                        subs.Add(RocksDbStorageMeta.From(iterator.StringValue()));
                    }
                    
                    iterator.Next();
                }

                foreach (var sub in subs.ToArray())
                {
                    if (sub.DbDirectory == dbDirectory)
                        continue;
                    
                    subs.AddRange(GetSubStorages(sub.DbDirectory, sub.StorageName));
                }
                
                return subs;
            }
            
            return GetSubStorages(this.dbDirectory, this.storageName).ToDictionary(k => k.StorageName, v => v);
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

            if (dbWithRefCount.IsUsed())
                return;
            
            dbWithRefCount.Db.Dispose(); // dispose the db
            RocksDbInstances.Remove(dbDirectory);
        }
        
        private string GetSubDatabasePath(string dbName) => $"{this.dbDirectory}{Path.DirectorySeparatorChar}{dbName}";
        
        private static bool IsDatabaseEmpty(RocksDb db)
        {
            using var iterator = db.NewIterator();
            iterator.SeekToFirst();
            
            using var iteratorSubStorageCf = db.NewIterator(cf: db.GetColumnFamily(subStoragesCFName));
            iteratorSubStorageCf.SeekToFirst();
            
            return iterator.Valid() && iteratorSubStorageCf.Valid();
        }

        private static RocksDb GetOrCreateRocksDbInstance(string dbDirectory)
        {
            if (RocksDbInstances.TryGetValue(dbDirectory, out var rocksDbInstance))
            {
                rocksDbInstance.IncrementRefCount();
                return rocksDbInstance.Db;
            }
            
            if (!Directory.Exists(dbDirectory))
            {
                Directory.CreateDirectory(dbDirectory);
            }
            
            var dbOptions = new DbOptions().SetCreateIfMissing().SetCreateMissingColumnFamilies();
            var columnFamilies = new ColumnFamilies
            {
                {subStoragesCFName, new ColumnFamilyOptions()}
            };
            
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

        private record RocksDbStorageMeta(string DbDirectory, string StorageName, string KeyPrefix)
        {
            public string DbDirectory { get; } = DbDirectory;
            public string StorageName { get; } = StorageName;
            public string KeyPrefix { get; } = KeyPrefix;

            public override string ToString()
            {
                return string.Join("|", DbDirectory, StorageName, KeyPrefix);
            }
            
            public static RocksDbStorageMeta From (string meta)
            {
                var parts = meta.Split('|');
                return new RocksDbStorageMeta(DbDirectory: parts[0], StorageName: parts[1], KeyPrefix: parts[2]);
            }
            
            public static RocksDbStorageMeta From (RocksDbStorage storage)
            {
                return new RocksDbStorageMeta(DbDirectory: storage.dbDirectory, StorageName: storage.storageName, KeyPrefix: storage.keyPrefix);
            }
        }
    }
}
