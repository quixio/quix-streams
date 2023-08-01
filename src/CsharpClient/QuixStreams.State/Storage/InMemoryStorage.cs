using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// Basic non-thread safe in-memory storage implementing <see cref="IStateStorage"/> interface.
    /// </summary>
    public class InMemoryStorage : IStateStorage
    {
        /// <summary>
        /// Represents the in-memory state holding the key-value pairs.
        /// </summary>
        private readonly IDictionary<string, byte[]> inMemoryState = new Dictionary<string, byte[]>();

        private readonly object subStateLock = new object();
        private readonly IDictionary<string, IStateStorage> subStates = new ConcurrentDictionary<string, IStateStorage>();
        
        /// <inheritdoc/>
        public Task SaveRaw(string key, byte[] data)
        {
            this.inMemoryState[key] = data;
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<byte[]> LoadRaw(string key)
        {
            return Task.FromResult(this.inMemoryState[key]);
        }

        /// <inheritdoc/>
        public Task RemoveAsync(string key)
        {
            this.inMemoryState.Remove(key);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<bool> ContainsKeyAsync(string key)
        {
            var contains = this.inMemoryState.ContainsKey(key);
            return Task.FromResult(contains);
        }

        /// <inheritdoc/>
        public Task<string[]> GetAllKeysAsync()
        {
            var keys = inMemoryState.Keys.ToArray();
            return Task.FromResult(keys);
        }

        /// <inheritdoc/>
        public Task ClearAsync()
        {
            this.inMemoryState.Clear();
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<int> Count()
        {
            return Task.FromResult(this.inMemoryState.Count);
        }

        /// <inheritdoc/>
        public bool IsCaseSensitive => true;

        /// <inheritdoc/>
        public IStateStorage GetOrCreateSubStorage(string subStorageName, string dbName = null)
        {
            if (this.subStates.TryGetValue(subStorageName, out var existing)) return existing;
            lock (this.subStateLock)
            {
                if (this.subStates.TryGetValue(subStorageName, out existing)) return existing;
                var storage = new InMemoryStorage();
                this.subStates[subStorageName] = storage;
                return storage;
            }
            
        }

        /// <inheritdoc/>
        public bool DeleteSubStorage(string subStorageName)
        {
            return this.subStates.Remove(subStorageName);
        }

        /// <inheritdoc/>
        public int DeleteSubStorages()
        {
            int count = 0;
            lock (this.subStateLock)
            {
                foreach (var sub in this.GetSubStorages())
                {
                    this.subStates.Remove(sub);
                    count++;
                }
            }

            return count;
        }

        /// <inheritdoc/>
        public IEnumerable<string> GetSubStorages()
        {
            return this.subStates.Keys.ToArray();
        }
        
        /// <inheritdoc/>
        public void StartTransaction() => throw new NotSupportedException();
        
        /// <inheritdoc/>
        public void CommitTransaction() => throw new NotSupportedException();
        
        /// <inheritdoc/>
        public bool CanPerformTransactions => false;
    }
}