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
    public class StreamDictionaryState : IStreamState, IDictionary<string, StateValue>, IDictionary
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.DictionaryState dictionaryState;

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
        internal StreamDictionaryState(IStateStorage storage, ILoggerFactory loggerFactory)
        {
            this.dictionaryState = new State.DictionaryState(storage, loggerFactory);
        }

        /// <inheritdoc/>
        public bool Contains(object key)
        {
            return this.ContainsKey((string)key);
        }

        /// <inheritdoc/>
        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return ((IDictionary)this.dictionaryState).GetEnumerator();
        }

        /// <inheritdoc/>
        public void Remove(object key)
        {
            this.Remove((string)key);
        }

        /// <inheritdoc/>
        public bool IsFixedSize => this.dictionaryState.IsFixedSize;

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, StateValue>> GetEnumerator()
        {
            return this.dictionaryState.GetEnumerator();
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
        public void Add(object key, object value)
        {
            this.Add((string)key, (StateValue)value);
        }

        /// <inheritdoc cref="IDictionary.Clear" />
        public void Clear()
        {
            this.dictionaryState.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, StateValue> item)
        {
            return this.ContainsKey(item.Key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, StateValue>[] array, int arrayIndex)
        {
            this.dictionaryState.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, StateValue> item)
        {
            return this.Remove(item.Key);
        }

        /// <inheritdoc/>
        public void CopyTo(Array array, int index)
        {
            this.dictionaryState.CopyTo(array, index);
        }

        /// <inheritdoc cref="ICollection.Count" />
        public int Count => this.dictionaryState.Count;

        /// <inheritdoc />
        public bool IsSynchronized => this.dictionaryState.IsSynchronized;
        
        /// <inheritdoc />
        public object SyncRoot => this.dictionaryState.SyncRoot;

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        public bool IsReadOnly => false;

        /// <inheritdoc />
        public object this[object key]
        {
            get => this[(string)key];
            set => this[(string)key] = (StateValue)value;
        }

        /// <inheritdoc/>
        public void Add(string key, StateValue value)
        {
            this.dictionaryState.Add(key, value);
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            return this.dictionaryState.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            return this.dictionaryState.Remove(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out StateValue value)
        {
            return dictionaryState.TryGetValue(key, out value);
        }

        /// <inheritdoc/>
        public StateValue this[string key]
        {
            get => this.dictionaryState[key];
            set => this.dictionaryState[key] = value;
        }

        /// <inheritdoc/>
        public ICollection<string> Keys => this.dictionaryState.Keys;

        /// <inheritdoc/>
        ICollection IDictionary.Values => ((IDictionary)this.dictionaryState).Values;

        /// <inheritdoc/>
        ICollection IDictionary.Keys => ((IDictionary)this.dictionaryState).Keys;

        /// <inheritdoc/>
        public ICollection<StateValue> Values => this.dictionaryState.Values;

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.dictionaryState.Flush();
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }
        
        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            this.dictionaryState.Reset();
        }
        
        public void Dispose()
        {
            this.dictionaryState.Dispose();
        }
    }
    
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    /// <typeparam name="T">The type of values stored in the StreamState.</typeparam>
    public class StreamDictionaryState<T> : IStreamState, IDictionary<string, T>
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.DictionaryState<T> dictionaryState;
        /// <summary>
        /// Returns whether the cache keys are case-sensitive
        /// </summary>
        private bool IsCaseSensitive => this.dictionaryState.IsCaseSensitive;
        
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
        internal StreamDictionaryState(IStateStorage storage, StreamStateDefaultValueDelegate<T> defaultValueFactory, ILoggerFactory loggerFactory)
        {
            this.dictionaryState = new State.DictionaryState<T>(storage, loggerFactory);
            this.defaultValueFactory = defaultValueFactory ?? (s => throw new KeyNotFoundException("The specified key was not found and there was no default value factory set."));
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        {
            return this.dictionaryState.GetEnumerator();
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
            this.dictionaryState.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, T> item)
        {
            return this.ContainsKey(item.Key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            this.dictionaryState.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, T> item)
        {
            return this.Remove(item.Key);
        }

        /// <inheritdoc/>
        public int Count => this.dictionaryState.Count;
        
        /// <inheritdoc/>
        public bool IsReadOnly => false;
        
        /// <inheritdoc/>
        public void Add(string key, T value)
        {
            this.dictionaryState.Add(key, value);
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            return this.dictionaryState.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            return this.dictionaryState.Remove(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T value)
        {
            return dictionaryState.TryGetValue(key, out value);
        }

        /// <inheritdoc/>
        public T this[string key]
        {
            get
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                if (this.TryGetValue(key, out T val)) return val;
                val = this.defaultValueFactory(key);
                this.dictionaryState[key] = val;
                return val;
            }
            set => this.dictionaryState[key] = value;
        }

        /// <inheritdoc/>
        public ICollection<string> Keys => this.dictionaryState.Keys;

        /// <inheritdoc/>
        public ICollection<T> Values => this.dictionaryState.Values;

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.dictionaryState.Flush();
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            this.dictionaryState.Reset();
        }
        
        public void Dispose()
        {
            this.dictionaryState.Dispose();
        }
    }
}