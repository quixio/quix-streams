using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RocksDbSharp;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// Key/Value storage using RocksDB that implements <see cref="IStateStorage"/> interface.
    /// </summary>
    public class RocksDbStorage : IStateStorage
    {
        /// <summary>
        /// Holds references of RocksDb instances to ensure only one exists and avoid IO lock issue.
        /// Key is the db directory and value is a tuple of RocksDb instance and reference count.
        /// </summary>
        private static readonly Dictionary<string, RocksDbInstance> RocksDbInstances = new ();

        private readonly RocksDb db;
        private readonly string dbDirectory;
        private readonly string keyPrefix;
        private readonly string storageName;

        private WriteBatch writeBatch = new();

        private const char Separator = '/';
        
        /// <summary>
        /// Instantiates a new instance of <see cref="RocksDbStorage"/>
        /// </summary>
        /// <param name="dbDirectory">The directory to open the database</param>
        /// <param name="streamId">Stream id of the storage</param>
        /// <param name="stateName">Stream name of the storage</param>
        public static RocksDbStorage GetStateStorage(string dbDirectory, string streamId, string stateName)
        {
            if (string.IsNullOrEmpty(dbDirectory) || string.IsNullOrEmpty(streamId) || string.IsNullOrEmpty(stateName))
            {
                throw new ArgumentException($"{nameof(dbDirectory)}, {nameof(streamId)} and {nameof(stateName)} cannot be null or empty.");
            }
            
            var storageName = $"{streamId}{Separator}{stateName}";
            return new RocksDbStorage(dbDirectory, storageName);
        }
        
        /// <summary>
        /// Deletes all the states of a stream
        /// </summary>
        /// <param name="dbDirectory">The directory to open the database</param>
        /// <param name="streamId">Stream id of the storage</param>
        public static void DeleteStreamStates(string dbDirectory, string streamId)
        {
            if (string.IsNullOrEmpty(dbDirectory) || string.IsNullOrEmpty(streamId))
            {
                throw new ArgumentException($"{nameof(dbDirectory)} and {nameof(streamId)} cannot be null or empty.");
            }
            
            using var streamStorage = new RocksDbStorage(dbDirectory, storageName: streamId);
            
            streamStorage.Clear();
        }
        
        /// <summary>
        /// Instantiates a new instance of <see cref="RocksDbStorage"/>
        /// </summary>
        /// <param name="dbDirectory">The directory to open the database</param>
        /// <param name="storageName">Name of the storage. Used as a prefix to separate data of other states when they use the same db</param>
        private RocksDbStorage(string dbDirectory, string storageName)
        {
            if (string.IsNullOrEmpty(dbDirectory) || string.IsNullOrEmpty(storageName))
            {
                throw new ArgumentException($"{nameof(dbDirectory)} and {nameof(storageName)} cannot be null or empty.");
            }
            
            this.db = GetOrCreateRocksDbInstance(dbDirectory);
            
            this.dbDirectory = dbDirectory;
            this.keyPrefix = $"{storageName}{Separator}";
            this.storageName = storageName;
        }
        
        /// <inheritdoc/>
        public Task SaveRaw(string key, byte[] data)
        {
            var byteKey = Encoding.UTF8.GetBytes($"{keyPrefix}{key}");
            writeBatch.Put(byteKey, data);
            
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
                            keys.Add(iterator.StringKey().Substring(this.keyPrefix.Length));
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
            var z = GetAllKeysAsync().GetAwaiter().GetResult();
            var cfHandle = string.IsNullOrEmpty(cf) ? db.GetDefaultColumnFamily() : db.GetColumnFamily(cf);
            var startKey = Encoding.UTF8.GetBytes(prefix);
            
            using var batch = new WriteBatch();
            using (var iterator = db.NewIterator())
            {
                iterator.Seek(prefix);
                while (iterator.Valid() && iterator.StringKey().StartsWith(prefix))
                {
                    batch.Delete(iterator.Key(), (ulong) iterator.Key().Length, cf: cfHandle);
                    iterator.Next();
                }
            }
            db.Write(batch);

            var x = GetAllKeysAsync().GetAwaiter().GetResult();
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
        public void Flush()
        {
            try
            {
                db.Write(writeBatch);
            }
            finally
            {
                writeBatch = new WriteBatch(); //As per documentation, creating a new object is recommended
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
            
            dbWithRefCount.Db.Dispose(); // closes and disposes the db
            RocksDbInstances.Remove(dbDirectory);
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
                // {stateStoragesCF, new ColumnFamilyOptions()}
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
    }
}
