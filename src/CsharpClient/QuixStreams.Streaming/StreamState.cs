using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming
{
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
        /// Returns whether the cache keys are case sensitive
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
    
    /// <summary>
    /// Manages the states of a stream.
    /// </summary>
    public class StreamStateManager
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly ILoggerFactory loggerFactory;
        private readonly string logPrefix;
        private readonly ILogger logger;
        private readonly string streamId;
        private readonly StreamStateStorageFactoryDelegate stateStorageFactory;

        private readonly object stateLock = new object();
        private readonly Dictionary<string, object> states = new Dictionary<string, object>();

        internal delegate IStateStorage StreamStateStorageFactoryDelegate(string streamId, string stateName);

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamStateManager"/> class with the specified parameters.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer used for committing state changes.</param>
        /// <param name="streamId">The ID of the stream.</param>
        /// <param name="stateStorageFactory">The factory delegate used for creating state storage.</param>
        /// <param name="loggerFactory">The logger factory used for creating loggers.</param>
        /// <param name="logPrefix">The prefix to be used in log messages.</param>
        internal StreamStateManager(ITopicConsumer topicConsumer, string streamId, StreamStateStorageFactoryDelegate stateStorageFactory, ILoggerFactory loggerFactory, string logPrefix)
        {
            this.topicConsumer = topicConsumer;
            this.loggerFactory = loggerFactory;
            this.logPrefix = $"{logPrefix}{streamId}";
            this.logger = this.loggerFactory.CreateLogger<StreamStateManager>();
            this.streamId = streamId;
            this.stateStorageFactory = stateStorageFactory;
        }
        
        /// <summary>
        /// Creates a new instance of the <see cref="StreamState{T}"/> class with the specified <paramref name="nameOfState"/> and optional <paramref name="defaultValueFactory"/>.
        /// </summary>
        /// <typeparam name="T">The type of data stored in the state.</typeparam>
        /// <param name="nameOfState">The name of the state.</param>
        /// <param name="defaultValueFactory">An optional delegate that returns a default value for the state if it does not exist.</param>
        /// <returns>The newly created <see cref="StreamState{T}"/> instance.</returns>
        private StreamState<T> CreateStreamState<T>(string nameOfState, StreamStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            var storage = stateStorageFactory(this.streamId, nameOfState);
            var state = new StreamState<T>(storage, defaultValueFactory, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {nameOfState}"));
            return state;
        }
        
        /// <summary>
        /// Creates a new application state with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="nameOfState">The name of the state</param>
        /// <param name="defaultValueFactory">The value factory for the state when the state has no value for the key</param>
        /// <returns>Stream state</returns>
        public StreamState<T> GetState<T>(string nameOfState, StreamStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            if (this.states.TryGetValue(nameOfState, out var existingState))
            {
                if (existingState.GetType().GetGenericArguments().First() != typeof(T))
                {
                    throw new ArgumentException($"{logPrefix}, State '{nameOfState}' already exists with a different type.");
                }

                return (StreamState<T>)existingState;
            }
            
            lock (stateLock)
            {
                if (this.states.TryGetValue(nameOfState, out existingState))
                {
                    if (existingState.GetType().GetGenericArguments().First() != typeof(T))
                    {
                        throw new ArgumentException($"{logPrefix}, State '{nameOfState}' already exists with a different type.");
                    }

                    return (StreamState<T>)existingState;
                }

                var state = CreateStreamState(nameOfState, defaultValueFactory);

                this.states.Add(nameOfState, state);
                if (this.topicConsumer == null) return state;
                var prefix = $"{logPrefix} - {nameOfState} | ";
                this.topicConsumer.OnCommitted += (sender, args) =>
                {
                    try
                    {
                        this.logger.LogTrace($"{prefix} | Flushing state.");
                        state.Flush();
                        this.logger.LogDebug($"{prefix} | Flushed state.");
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogError(ex, $"{prefix} | Failed to flush state.");
                    }

                };
                return state;
            }
        }
        
    }
}