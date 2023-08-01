using System.Collections.Generic;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// The minimum definition for a state storage
    /// </summary>
    public interface IStateStorage
    {
        /// <summary>
        /// Save raw data into the key
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <param name="data">Raw byte[] representation of data</param>
        /// <returns>Awaitable task</returns>
        public Task SaveRaw(string key, byte[] data);

        /// <summary>
        /// Load raw data from the key
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <returns>Awaitable result for raw byte[] representation of data</returns>
        public Task<byte[]> LoadRaw(string key);

        /// <summary>
        /// Remove key from the storage
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <returns>Awaitable task</returns>
        public Task RemoveAsync(string key);

        /// <summary>
        /// Check if storage contains key
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <returns>Awaitable result for boolean representing whether the storage contains key</returns>
        public Task<bool> ContainsKeyAsync(string key);

        /// <summary>
        /// Get list of all keys in the storage
        /// This function is written in the asynchronous manner
        /// </summary>
        /// <returns>Awaitable result for the keys as a hash set</returns>
        public Task<string[]> GetAllKeysAsync();

        /// <summary>
        /// Clear the storage / remove all keys from the storage
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        /// <returns>Awaitable task</returns>
        public Task ClearAsync();

        /// <summary>
        /// Returns the number of keys in storage
        /// </summary>
        /// <returns></returns>
        public Task<int> Count();
        
        /// <summary>
        /// Returns whether the storage is case-sensitive
        /// </summary>
        public bool IsCaseSensitive { get; }
        
        /// <summary>
        /// Gets an existing sub-storage with the specified name or creates a new one if it does not already exist.
        /// </summary>
        /// <param name="subStorageName">The name of the sub-storage to retrieve or create.</param>
        /// <param name="dbName">The name of the database under which the storage will be created. 
        /// If this parameter is not specified, the storage will be created under the parent's database.</param>
        /// <returns>Returns the state storage associated with the given sub-storage name.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="subStorageName"/> is null or empty.</exception>
        public IStateStorage GetOrCreateSubStorage(string subStorageName, string dbName = null);

        /// <summary>
        /// Deletes a storage under this in hierarchy.
        /// </summary>
        /// <param name="subStorageName">The name of the sub storage</param>
        /// <returns>Whether the state storage for the given storage name was deleted</returns>
        public bool DeleteSubStorage(string subStorageName);

        /// <summary>
        /// Deletes the storages under this in hierarchy.
        /// </summary>
        /// <returns>The number of state storage deleted</returns>
        public int DeleteSubStorages();

        /// <summary>
        /// Gets the storages under this in hierarchy.
        /// </summary>
        /// <returns>The enumerable storage names this store contains</returns>
        public IEnumerable<string> GetSubStorages();
        
        /// <summary>
        /// Returns whether the transactions are supported
        /// </summary>
        public bool CanPerformTransactions { get; }
        
        /// <summary>
        /// Starts a transaction
        /// </summary>
        public void StartTransaction();
        
        /// <summary>
        /// Commits a transaction
        /// </summary>
        /// <exception>Throws exception if the transaction fails</exception>
        public void CommitTransaction();
    }
}