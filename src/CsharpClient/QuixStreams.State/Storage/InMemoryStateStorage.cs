using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// Basic non-thread safe in-memory storage implementing <see cref="IStateStorage"/> interface.
    /// </summary>
    public class InMemoryStateStorage : IStateStorage
    {
        /// <summary>
        /// Represents the in-memory state holding the key-value pairs.
        /// </summary>
        private readonly IDictionary<string, byte[]> inMemoryState = new Dictionary<string, byte[]>();
        
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
    }
}