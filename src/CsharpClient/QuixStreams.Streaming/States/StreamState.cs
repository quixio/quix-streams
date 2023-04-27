using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using QuixStreams.State;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    public class StreamState : IDictionary<string, StateValue>
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.State state;
        
        /// <summary>
        /// Returns whether the cache keys are case-sensitive
        /// </summary>

        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the StreamState class.
        /// </summary>
        /// <param name="storage">The storage the stream state is going to use as underlying storage</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal StreamState(IStateStorage storage, ILoggerFactory loggerFactory)
        {
            this.state = new State.State(storage, loggerFactory);
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, StateValue>> GetEnumerator()
        {
            return this.state.GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc/>
        public void Add(KeyValuePair<string, StateValue> item)
        {
            this.Add(item.Key, item.Value);
        }

        /// <inheritdoc/>
        public void Clear()
        {
            this.state.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, StateValue> item)
        {
            return this.ContainsKey(item.Key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, StateValue>[] array, int arrayIndex)
        {
            this.state.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, StateValue> item)
        {
            return this.Remove(item.Key);
        }

        /// <inheritdoc/>
        public int Count => this.state.Count;
        
        /// <inheritdoc/>
        public bool IsReadOnly => false;
        
        /// <inheritdoc/>
        public void Add(string key, StateValue value)
        {
            this.state.Add(key, value);
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            return this.state.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            return this.state.Remove(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out StateValue value)
        {
            return state.TryGetValue(key, out value);
        }

        /// <inheritdoc/>
        public StateValue this[string key]
        {
            get => this.state[key];
            set => this.state[key] = value;
        }

        /// <inheritdoc/>
        public ICollection<string> Keys => this.state.Keys;

        /// <inheritdoc/>
        public ICollection<StateValue> Values => this.state.Values;

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.state.Flush();
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }
    }
    
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    /// <typeparam name="T">The type of values stored in the StreamState.</typeparam>
    public class StreamState<T> : IDictionary<string, T>
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.State<T> state;
        /// <summary>
        /// Returns whether the cache keys are case-sensitive
        /// </summary>
        private bool IsCaseSensitive => this.state.IsCaseSensitive;
        
        /// <summary>
        /// A function that takes a string key and returns a default value of type T when the key is not found in the state storage.
        /// </summary>
        private readonly StreamStateDefaultValueDelegate<T> defaultValueFactory;

        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the StreamState class.
        /// </summary>
        /// <param name="storage">The storage the stream state is going to use as underlying storage</param>
        /// <param name="defaultValueFactory">A function that takes a string key and returns a default value of type T when the key is not found in the state</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal StreamState(IStateStorage storage, StreamStateDefaultValueDelegate<T> defaultValueFactory, ILoggerFactory loggerFactory)
        {
            this.state = new State.State<T>(storage, loggerFactory);
            this.defaultValueFactory = defaultValueFactory ?? (s => throw new KeyNotFoundException("The specified key was not found and there was no default value factory set."));
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        {
            return this.state.GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc/>
        public void Add(KeyValuePair<string, T> item)
        {
            this.Add(item.Key, item.Value);
        }

        /// <inheritdoc/>
        public void Clear()
        {
            this.state.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, T> item)
        {
            return this.ContainsKey(item.Key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            this.state.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, T> item)
        {
            return this.Remove(item.Key);
        }

        /// <inheritdoc/>
        public int Count => this.state.Count;
        
        /// <inheritdoc/>
        public bool IsReadOnly => false;
        
        /// <inheritdoc/>
        public void Add(string key, T value)
        {
            this.state.Add(key, value);
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            return this.state.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            return this.state.Remove(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T value)
        {
            return state.TryGetValue(key, out value);
        }

        /// <inheritdoc/>
        public T this[string key]
        {
            get
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                if (this.TryGetValue(key, out T val)) return val;
                val = this.defaultValueFactory(key);
                this.state[key] = val;
                return val;
            }
            set => this.state[key] = value;
        }

        /// <inheritdoc/>
        public ICollection<string> Keys => this.state.Keys;

        /// <inheritdoc/>
        public ICollection<T> Values => this.state.Values;

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.state.Flush();
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }
    }
    
    /// <summary>
    /// Represents a method that returns the default value for a given key.
    /// </summary>
    /// <typeparam name="T">The type of the default value.</typeparam>
    /// <param name="key">The name of the key being accessed.</param>
    /// <returns>The default value for the specified key.</returns>
    public delegate T StreamStateDefaultValueDelegate<out T>(string key);
}