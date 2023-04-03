using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using QuixStreams.State.Storage;

namespace QuixStreams.State
{
    /// <summary>
    /// Represents a state container that stores key-value pairs with the ability to flush changes to a specified storage.
    /// </summary>
    public class State
    {
        /// <summary>
        /// Represents the storage where the state changes will be persisted.
        /// </summary>
        private readonly IStateStorage storage;
        
        /// <summary>
        /// Indicates whether the storage should be cleared before flushing the changes.
        /// </summary>
        private bool clearBeforeFlush = false;
        
        /// <summary>
        /// Represents the in-memory state holding the key-value pairs.
        /// </summary>
        private readonly IDictionary<string, StateValue> inMemoryState = new Dictionary<string, StateValue>();
        
        /// <summary>
        /// Represents the in-memory state holding the key-value pairs of last persisted lazy values
        /// </summary>
        private readonly IDictionary<string, int> lastFlushHash = new Dictionary<string, int>();
        
        /// <summary>
        /// Represents the changes made to the in-memory state, tracking additions, updates, and removals.
        /// </summary>
        private readonly IDictionary<string, ChangeType> changes = new Dictionary<string, ChangeType>();

        /// <summary>
        /// Returns whether the cache keys are case sensitive
        /// </summary>
        public bool IsCaseSensitive => this.storage.IsCaseSensitive; 

        /// <summary>
        /// Initializes a new instance of the <see cref="State"/> class using the specified storage.
        /// </summary>
        /// <param name="storage">An instance of <see cref="IStateStorage"/> that represents the storage to persist state changes to.</param>
        /// <exception cref="ArgumentNullException">Thrown when the storage parameter is null.</exception>
        public State(IStateStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            var keys = this.storage.GetAllKeys();
            foreach (var key in keys)
            {
                inMemoryState[key] = this.storage.Get(key);
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the in-memory state.
        /// </summary>
        /// <returns>An enumerator for the in-memory state.</returns>
        public IEnumerator<KeyValuePair<string, StateValue>> GetEnumerator()
        {
            return inMemoryState.GetEnumerator();
        }

        /// <summary>
        /// Removes all key-value pairs from the in-memory state and marks the state for clearing when flushed.
        /// </summary>
        public void Clear()
        {
            inMemoryState.Clear();
            changes.Clear();
            clearBeforeFlush = true;
        }

        /// <summary>
        /// Gets the number of key-value pairs contained in the in-memory state.
        /// </summary>
        public int Count => inMemoryState.Count;
        
        /// <summary>
        /// Adds the specified key and value to the in-memory state and marks the entry for addition or update when flushed.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">The value of the element to add.</param>
        public void Add(string key, StateValue value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            inMemoryState.Add(key, value);
            this.changes[key] = ChangeType.AddedOrUpdated;
        }

        /// <summary>
        /// Determines whether the in-memory state contains an element with the specified key.
        /// </summary>
        /// <param name="key">The key to locate in the in-memory state.</param>
        /// <returns>true if the in-memory state contains an element with the key; otherwise, false.</returns>
        public bool ContainsKey(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            return inMemoryState.ContainsKey(key);
        }

        /// <summary>
        /// Removes the element with the specified key from the in-memory state and marks the entry for removal when flushed.
        /// </summary>
        /// <param name="key">The key of the element to remove.</param>
        /// <returns>true if the element is successfully removed; otherwise, false.</returns>
        public bool Remove(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            var success = inMemoryState.Remove(key);
            if (success) this.changes[key] = ChangeType.Removed;
            return success;
        }

        /// <summary>
        /// Gets the value associated with the specified key from the in-memory state.
        /// </summary>
        /// <param name="key">The key of the value to get.</param>
        /// <param name="value">When this method returns, the value associated with the specified key, if the key is found; otherwise, the default value for the type of the value parameter.</param>
        /// <returns>true if the in-memory state contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue(string key, out StateValue value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            return inMemoryState.TryGetValue(key, out value);
        }

        /// <summary>
        /// Gets or sets the element with the specified key in the in-memory state.
        /// </summary>
        /// <param name="key">The key of the element to get or set.</param>
        /// <returns>The element with the specified key.</returns>
        public StateValue this[string key]
        {
            get
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                return this.inMemoryState[key];
            }
            set
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                if (value == null) this.changes[key] = ChangeType.Removed;
                else this.changes[key] = ChangeType.AddedOrUpdated;
                this.inMemoryState[key] = value;
            }
        }

        /// <summary>
        /// Gets an ICollection containing the keys of the in-memory state.
        /// </summary>
        public ICollection<string> Keys => this.inMemoryState.Keys;
        
        /// <summary>
        /// Gets an ICollection containing the values of the in-memory state.
        /// </summary>
        public ICollection<StateValue> Values => this.inMemoryState.Values;

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            if (this.clearBeforeFlush)
            {
                this.storage.Clear();
                this.clearBeforeFlush = false;
            }

            var tasks = new List<Task>();
            foreach (var changeType in changes)
            {
                if (changeType.Value == ChangeType.Removed)
                {
                    this.lastFlushHash.Remove(changeType.Key);
                    tasks.Add(this.storage.RemoveAsync(changeType.Key));
                }
                else
                {
                    var hash = inMemoryState[changeType.Key].GetHashCode();
                    if (lastFlushHash.TryGetValue(changeType.Key, out var existingHash) && existingHash == hash) continue;
                    this.lastFlushHash[changeType.Key] = hash;
                    tasks.Add(this.storage.SetAsync(changeType.Key, inMemoryState[changeType.Key]));
                }
            }
            
            this.changes.Clear();
            Task.WaitAll(tasks.ToArray());
        }

        /// <summary>
        /// Represents the types of changes made to the state.
        /// </summary>
        private enum ChangeType
        {
            Removed,
            AddedOrUpdated
        }
    }
}