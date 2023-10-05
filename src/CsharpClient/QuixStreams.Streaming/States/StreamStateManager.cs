using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.Models;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Manages the states.
    /// </summary>
    public class StreamStateManager
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly ILoggerFactory loggerFactory;
        private readonly string logPrefix;
        private readonly ILogger logger;

        private readonly object stateLock = new object();
        private readonly Dictionary<string, IStreamState> states = new Dictionary<string, IStreamState>();
        private readonly string streamId;
        
        private static readonly ConcurrentDictionary<StreamConsumerId, StreamStateManager> StreamStateManagers = new ConcurrentDictionary<StreamConsumerId, StreamStateManager>();
        
        /// <summary>
        /// The directory where the states are stored on disk. 
        /// </summary>
        public readonly string StorageDir;
        
        /// <summary>
        /// Initializes or gets an existing instance of the <see cref="StreamStateManager"/> class with the specified parameters.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer used for committing state changes.</param>
        /// <param name="streamConsumerId">Stream consumer identifier information.</param>
        /// <param name="loggerFactory">The logger factory used for creating loggers.</param>
        public static StreamStateManager GetOrCreate(ITopicConsumer topicConsumer, StreamConsumerId streamConsumerId, ILoggerFactory loggerFactory)
        {
            return StreamStateManagers.GetOrAdd(streamConsumerId,
                (key) => new StreamStateManager(topicConsumer, key, loggerFactory));
        }

        /// <summary>
        /// Tries to revoke the stream state manager for the specified stream consumer id.
        /// </summary>
        /// <param name="streamConsumerId"></param>
        /// <returns></returns>
        public static bool TryRevoke(StreamConsumerId streamConsumerId)
        {
            if (StreamStateManagers.TryRemove(streamConsumerId, out var stateManager))
            {
                foreach (var stateName in stateManager.states.Keys)
                {
                    stateManager.TryDisposeStreamState(stateName);
                }

                return true;
            }

            return false;
        }
        
        private StreamStateManager(ITopicConsumer topicConsumer, StreamConsumerId streamConsumerId, ILoggerFactory loggerFactory)
        {
            this.topicConsumer = topicConsumer;
            this.loggerFactory = loggerFactory;
            this.streamId = streamConsumerId.StreamId;
            this.logPrefix = $"{streamConsumerId.ConsumerGroup}-{streamConsumerId.TopicName}-{streamConsumerId.Partition}-{streamId}";
            this.logger = this.loggerFactory.CreateLogger<StreamStateManager>();
            this.StorageDir = Path.Combine(App.GetStateStorageRootDir(), streamConsumerId.ConsumerGroup, streamConsumerId.TopicName, streamConsumerId.Partition.ToString());

            if (this.topicConsumer != null)
            {
                topicConsumer.OnDisposed += (sender, args) => { TryRevoke(streamConsumerId); };
            }
        }
        
        /// <summary>
        /// Deletes all states for the current stream.
        /// </summary>
        public void DeleteStates()
        {
            this.logger.LogTrace("Deleting Stream states for {0}", streamId);
            
            if (App.GetStateStorageType() == StateStorageTypes.RocksDb)
            {
                RocksDbStorage.DeleteStreamStates(StorageDir, streamId);
            }
            else if (App.GetStateStorageType() == StateStorageTypes.InMemory)
            {
                InMemoryStorage.DeleteStreamStates(streamId);
            }
            else
            {
                throw new ArgumentException("Invalid state storage type");
            }

            foreach (var stateName in this.states.Keys)
            {
                TryDisposeStreamState(stateName);
            }
        }

        /// <summary>
        /// Deletes the state with the specified name
        /// </summary>
        /// <param name="stateName">The state to delete</param>
        public void DeleteState(string stateName)
        {
            this.logger.LogTrace("Deleting Stream state {0} for {1}", stateName, streamId);
            TryDisposeStreamState(stateName);
            var storage = GetStateStorage(stateName);
            storage.Clear();
            storage.Dispose();
        }

        /// <summary>
        /// Creates a new application state of dictionary type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <returns>Dictionary stream state</returns>
        public StreamDictionaryState GetDictionaryState(string stateName)
        {
            return GetStreamState(stateName, (stateName) =>
            {
                this.logger.LogTrace("Creating Stream state for {0}", stateName);
                var storage = GetStateStorage(stateName);
                var state = new StreamDictionaryState(storage, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }

        /// <summary>
        /// Creates a new application state of dictionary type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <param name="defaultValueFactory">The value factory for the state when the state has no value for the key</param>
        /// <returns>Dictionary stream state</returns>
        public StreamDictionaryState<T> GetDictionaryState<T>(string stateName,
            StreamStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            return GetStreamState(stateName, (stateName) =>
            {
                this.logger.LogTrace("Creating Stream state for {0}", stateName);
                var storage = GetStateStorage(stateName);
                var state = new StreamDictionaryState<T>(storage,
                    defaultValueFactory, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }

        /// <summary>
        /// Creates a new application state of scalar type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <returns>Dictionary stream state</returns>
        public StreamScalarState GetScalarState(string stateName)
        {
            return GetStreamState(stateName, (stateName) =>
            {
                this.logger.LogTrace("Creating Stream state for {0}", stateName);
                var storage = GetStateStorage(stateName);
                var state = new StreamScalarState(storage,
                    new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }

        /// <summary>
        /// Creates a new application state of scalar type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <param name="defaultValueFactory">The value factory for the state when the state has no value for the key</param>
        /// <returns>Dictionary stream state</returns>
        public StreamScalarState<T> GetScalarState<T>(string stateName,
            StreamStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            return GetStreamState(stateName, (stateName) =>
            {
                this.logger.LogTrace("Creating Stream state for {0}", stateName);
                var storage = GetStateStorage(stateName);
                var state = new StreamScalarState<T>(storage,
                    defaultValueFactory, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }

        /// <summary>
        /// Creates a new stream state of given type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <param name="createState">The function to create the state</param>
        /// <typeparam name="TState">The type of the stream state</typeparam>
        /// <returns>Stream state</returns>
        private TState GetStreamState<TState>(string stateName, Func<string, TState> createState)
            where TState : IStreamState
        {
            if (this.states.TryGetValue(stateName, out var existingState))
            {
                if (!(existingState is TState castedExistingState))
                {
                    throw new ArgumentException(
                        $"{logPrefix}, State '{stateName}' already exists as different stream state type.");
                }

                return castedExistingState;
            }

            lock (stateLock)
            {
                if (this.states.TryGetValue(stateName, out existingState))
                {
                    if (!(existingState is TState castedExistingState))
                    {
                        throw new ArgumentException(
                            $"{logPrefix}, State '{stateName}' already exists as different stream state type.");
                    }

                    return castedExistingState;
                }

                var state = createState(stateName);

                this.states.Add(stateName, state);
                if (this.topicConsumer == null) return state;
                var prefix = $"{logPrefix} - {stateName} | ";

                void CommittedHandler(object sender, EventArgs args)
                {
                    try
                    {
                        this.logger.LogDebug($"{prefix} | Topic committing, flushing state.");
                        state.Flush();
                        this.logger.LogTrace($"{prefix} | Topic committing, flushed state.");
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogError(ex, $"{prefix} | Failed to flush state.");
                    }
                }

                this.topicConsumer.OnCommitted += CommittedHandler;

                this.topicConsumer.OnStreamsRevoked += (sender, consumers) =>
                {
                    if (consumers.All(y => y.StreamId != streamId)) return;
                    this.logger.LogDebug($"{prefix} | Stream revoked, discarding state.");
                    this.topicConsumer.OnCommitted -= CommittedHandler;
                    state.Reset();
                    TryDisposeStreamState(stateName);
                };
                return state;
            }
        }
        
        private bool TryDisposeStreamState(string stateName)
        {
            if (this.states.Remove(stateName, out var state))
            {
                state.Dispose(); // Disposes the storage
                return true;
            }

            return false;
        }
        
        private IStateStorage GetStateStorage(string stateName)
        {
            if (App.GetStateStorageType() == StateStorageTypes.RocksDb)
            {
                return RocksDbStorage.GetStateStorage(StorageDir, streamId, stateName);
            }
            else if (App.GetStateStorageType() == StateStorageTypes.InMemory)
            {
                return InMemoryStorage.GetStateStorage(streamId, stateName);
            }
            else
            {
                throw new ArgumentException("Invalid state storage type");
            }
        }
    }
}