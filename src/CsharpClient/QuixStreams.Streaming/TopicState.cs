using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.State.Storage;
using QuixStreams.State.Storage.FileStorage.LocalFileStorage;
using QuixStreams.Streaming.Models;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    /// <typeparam name="T">The type of values stored in the TopicState.</typeparam>
    public class TopicState<T> : IDictionary<string, T>
    {
        /// <summary>
        /// The logger for the class
        /// </summary>
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<TopicState<T>>();

        /// <summary>
        /// The underlying state storage for this TopicState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.State<T> state;
        /// <summary>
        /// Returns whether the cache keys are case sensitive
        /// </summary>
        private bool IsCaseSensitive => this.state.IsCaseSensitive;
        
        /// <summary>
        /// A function that takes a string key and returns a default value of type T when the key is not found in the state storage.
        /// </summary>
        private readonly TopicStateDefaultValueDelegate<T> defaultValueFactory;

        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the TopicState class.
        /// </summary>
        /// <param name="storage">The storage to use for the underlying state</param>
        /// <param name="defaultValueFactory">A function that takes a string key and returns a default value of type T when the key is not found in the state</param>
        /// <param name="loggerFactory">The optional logging factory</param>
        public TopicState(IStateStorage storage, TopicStateDefaultValueDelegate<T> defaultValueFactory, ILoggerFactory loggerFactory = null)
        {
            this.state = new State.State<T>(storage, Logging.Factory);
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
            logger.LogTrace("Flushing state");
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.state.Flush();
            logger.LogTrace("Flushed state");
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }
    }
    
    /// <summary>
    /// Represents a method that returns the default value for a given key.
    /// </summary>
    /// <typeparam name="T">The type of the default value.</typeparam>
    /// <param name="key">The name of the key being accessed.</param>
    /// <returns>The default value for the specified key.</returns>
    public delegate T TopicStateDefaultValueDelegate<out T>(string key);
    
    /// <summary>
    /// Manages the states of a topic.
    /// </summary>
    public class TopicStateManager
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly string topicName;
        private readonly ILoggerFactory loggerFactory;

        private readonly object stateLock = new object();
        private readonly Dictionary<string, object> states = new Dictionary<string, object>();
        
        private readonly object streamStateManagerLock = new object();
        private readonly Dictionary<string, StreamStateManager> streamStateManagers = new Dictionary<string, StreamStateManager>();

        private readonly ILogger<TopicStateManager> logger;
        private const string BasePath = "state";
        private const string StreamStatePrefix = "__stream_";

        /// <summary>
        /// Initializes a new instance of the TopicStateManager class.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer this manager is for.</param>
        /// <param name="topicName">The name of the topic</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal TopicStateManager(ITopicConsumer topicConsumer, string topicName, ILoggerFactory loggerFactory)
        {
            this.topicConsumer = topicConsumer;
            this.topicName = topicName;
            this.loggerFactory = loggerFactory;
            this.logger = this.loggerFactory.CreateLogger<TopicStateManager>();
        }

        /// <summary>
        /// Creates a new instance of the <see cref="TopicState{T}"/> class with the specified <paramref name="nameOfState"/> and optional <paramref name="defaultValueFactory"/>.
        /// </summary>
        /// <typeparam name="T">The type of value stored in the <see cref="TopicState{T}"/>.</typeparam>
        /// <param name="nameOfState">The name of the state.</param>
        /// <param name="defaultValueFactory">An optional delegate that returns a default value for the state if it does not exist.</param>
        /// <returns>The newly created <see cref="TopicState{T}"/> instance.</returns>
        private TopicState<T> CreateTopicState<T>(string nameOfState, TopicStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            // TODO possibly refactor to be able to allow using different storage
            var fileStorage = new LocalFileStorage($"{BasePath}{Path.DirectorySeparatorChar}{this.topicName}{Path.DirectorySeparatorChar}{nameOfState}");
            var state = new TopicState<T>(fileStorage, defaultValueFactory, Logging.CreatePrefixedFactory($"{this.topicName} - {nameOfState}"));
            return state;
        }

        /// <summary>
        /// Returns an enumerable collection of all available stream state ids for the current topic.x  
        /// </summary>
        /// <returns>An enumerable collection of string values representing the ids of the stream states.</returns>
        public IEnumerable<string> GetStreamStates()
        {
            // TODO rework this to move work to FileStorage & expose on interface the action to list sub-storages with prefix
            var topicPath = $"{BasePath}{Path.DirectorySeparatorChar}{this.topicName}";
            if (!Directory.Exists(topicPath)) yield break;
            foreach (var dirPath in Directory.EnumerateDirectories(topicPath))
            {
                var dirName = Path.GetFileName(dirPath); // this just gets the last segment
                if (string.IsNullOrWhiteSpace(dirName)) continue;
                if (dirName.StartsWith(StreamStatePrefix)) yield return dirName.Substring(StreamStatePrefix.Length);
            }
        }
        
        /// <summary>
        /// Deletes all stream states for the current topic.
        /// </summary>
        /// <returns>The number of stream states that were deleted.</returns>
        public int DeleteStreamStates()
        {
            // TODO rework this to move work to FileStorage & expose on interface the action to delete sub-storages with prefix
            var topicPath = $"{BasePath}{Path.DirectorySeparatorChar}{this.topicName}";
            if (!Directory.Exists(topicPath)) return 0;
            var totalDeleted = 0;
            foreach (var dirPath in Directory.EnumerateDirectories(topicPath))
            {
                var dirName = Path.GetFileName(dirPath);
                if (string.IsNullOrWhiteSpace(dirName)) continue;
                if (!dirName.StartsWith(StreamStatePrefix)) continue;
                totalDeleted++;
                Directory.Delete(dirPath, true);
            }

            return totalDeleted;
        }

        /// <summary>
        /// Creates a new instance of the <see cref="LocalFileStorage"/> class with the specified <paramref name="streamId"/> and <paramref name="nameOfState"/>.
        /// </summary>
        /// <param name="streamId">The ID of the stream.</param>
        /// <param name="nameOfState">The name of the state.</param>
        /// <returns>The newly created <see cref="LocalFileStorage"/> instance.</returns>
        private IStateStorage CreateStreamStateStorage(string streamId, string nameOfState)
        {
            // TODO possibly refactor to be able to allow using different storage
            var fileStorage = new LocalFileStorage($"{BasePath}{Path.DirectorySeparatorChar}{this.topicName}{Path.DirectorySeparatorChar}{StreamStatePrefix}{streamId}{Path.DirectorySeparatorChar}{nameOfState}");
            return fileStorage;
        }

        /// <summary>
        /// Creates a new application state with automatically managed lifecycle for the topic
        /// </summary>
        /// <param name="nameOfState">The name of the state</param>
        /// <param name="defaultValueFactory">The value factory for the state when the state has no value for the key</param>
        /// <returns>Topic state</returns>
        public TopicState<T> GetState<T>(string nameOfState, TopicStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            if (this.states.TryGetValue(nameOfState, out var existingState))
            {
                if (existingState.GetType().GetGenericArguments().First() != typeof(T))
                {
                    throw new ArgumentException($"State '{nameOfState}' for topic '{this.topicName}' already exists with a different type.");
                }

                return (TopicState<T>)existingState;
            }
            
            lock (stateLock)
            {
                if (this.states.TryGetValue(nameOfState, out existingState))
                {
                    if (existingState.GetType().GetGenericArguments().First() != typeof(T))
                    {
                        throw new ArgumentException($"State '{nameOfState}' for topic '{this.topicName}' already exists with a different type.");
                    }

                    return (TopicState<T>)existingState;
                }

                var state = CreateTopicState(nameOfState, defaultValueFactory);

                this.states.Add(nameOfState, state);
                if (this.topicConsumer == null) return state;
                this.topicConsumer.OnCommitted += (sender, args) =>
                {
                    try
                    {
                        this.logger.LogTrace("Flushing state '{0}' for topic '{1}'.", nameOfState, this.topicName);
                        state.Flush();
                        this.logger.LogDebug("Flushed state '{0}' for topic '{1}'.", nameOfState, this.topicName);
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogError(ex, "Failed to flush state '{0}' for topic '{1}'.", nameOfState, this.topicName);
                    }

                };
                return state;
            }
        }
        
        /// <summary>
        /// Gets a new instance of the <see cref="StreamStateManager"/> class for the specified <paramref name="streamId"/>.
        /// </summary>
        /// <param name="streamId">The ID of the stream.</param>
        /// <returns>The newly created <see cref="StreamStateManager"/> instance.</returns>
        public StreamStateManager GetStreamStateManager(string streamId)
        {
            if (this.streamStateManagers.TryGetValue(streamId, out var existing)) return existing;
            lock (streamStateManagerLock)
            {
                if (this.streamStateManagers.TryGetValue(streamId, out existing)) return existing;
                var manager = new StreamStateManager(this.topicConsumer, streamId, this.CreateStreamStateStorage, this.loggerFactory, this.topicName + " ");
                this.streamStateManagers[streamId] = manager;
                return manager;
            }
        }
    }
}