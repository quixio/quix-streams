using System;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// The minimum definition for a state storage
    /// </summary>
    public interface IStateStorage: IDisposable
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
        /// Returns whether the transactions are supported
        /// </summary>
        public bool CanPerformTransactions { get; }
        
        /// <summary>
        /// Flush data to the storage
        /// </summary>
        /// <exception>Throws exception if the transaction fails</exception>
        public void Flush();
    }
    
    /// <summary>
    /// State storage types
    /// </summary>
    public enum StateStorageTypes
    {
        /// <summary>
        /// RocksDB storage
        /// </summary>
        RocksDb,
        
        /// <summary>
        /// In-memory storage
        /// </summary>
        InMemory
    }
}