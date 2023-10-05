using System;
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
        
        private readonly string keyPrefix;
        private readonly string storageName;
        
        private const char Separator = '/';
        
        /// <summary>
        /// Instantiates a new instance of <see cref="InMemoryStorage"/>
        /// </summary>
        /// <param name="streamId">Stream id of the storage</param>
        /// <param name="stateName">Stream id of the storage</param>
        public static InMemoryStorage GetStateStorage(string streamId, string stateName)
        {
            if (string.IsNullOrEmpty(streamId) || string.IsNullOrEmpty(stateName))
            {
                throw new ArgumentException($"{nameof(streamId)} and {nameof(stateName)} cannot be null or empty.");
            }
            
            var storageName = $"{streamId}{Separator}{stateName}";
            return new InMemoryStorage(storageName);
        }
        
        /// <summary>
        /// Instantiates a new instance of <see cref="InMemoryStorage"/>
        /// </summary>
        /// <param name="storageName">Name of the storage. Used as a prefix to separate data of other states when they use the same db</param>
        private InMemoryStorage(string storageName)
        {
            if (string.IsNullOrEmpty(storageName))
            {
                throw new ArgumentException($"{nameof(storageName)} cannot be null or empty.");
            }
            
            this.keyPrefix = $"{storageName}{Separator}";
            this.storageName = storageName;
        }
        
        /// <inheritdoc/>
        public Task SaveRaw(string key, byte[] data)
        {
            var prefixedKey = this.keyPrefix + key;
            this.inMemoryState[prefixedKey] = data;
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<byte[]> LoadRaw(string key)
        {
            var prefixedKey = this.keyPrefix + key;
            return Task.FromResult(this.inMemoryState[prefixedKey]);
        }

        /// <inheritdoc/>
        public Task RemoveAsync(string key)
        {
            var prefixedKey = this.keyPrefix + key;
            this.inMemoryState.Remove(prefixedKey);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task<bool> ContainsKeyAsync(string key)
        {
            var prefixedKey = this.keyPrefix + key;
            var contains = this.inMemoryState.ContainsKey(prefixedKey);
            return Task.FromResult(contains);
        }

        /// <inheritdoc/>
        public Task<string[]> GetAllKeysAsync()
        {
            var keys = inMemoryState.Keys.Select(x => x.Substring(this.keyPrefix.Length)).ToArray();
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
        public void Flush() => throw new NotSupportedException();
        
        /// <inheritdoc/>
        public bool CanPerformTransactions => false;
        
        /// <summary>
        /// Deletes all the states of a stream
        /// </summary>
        /// <param name="streamId">Stream id of the storage</param>
        public static void DeleteStreamStates(string streamId)
        {
            if (string.IsNullOrEmpty(streamId))
            {
                throw new ArgumentException($"{nameof(streamId)} cannot be null or empty.");
            }
            
            var streamStorage = new InMemoryStorage(storageName: streamId);
            streamStorage.Clear();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            // Nothing to dispose
        }
    }
}