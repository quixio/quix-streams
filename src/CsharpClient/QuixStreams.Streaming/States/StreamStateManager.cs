using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    
    //consumer_group/topic/<partition_num>.db
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
        private readonly string storageDir;


        /// <summary>
        /// Initializes a new instance of the <see cref="StreamStateManager"/> class with the specified parameters.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer used for committing state changes.</param>
        /// <param name="streamId">The ID of the stream.</param>
        /// <param name="stateStorage">The state storage to use.</param>
        /// <param name="loggerFactory">The logger factory used for creating loggers.</param>
        /// <param name="logPrefix">The prefix to be used in log messages.</param>
        internal StreamStateManager(ITopicConsumer topicConsumer, string streamId, string consumerGroup, string topicName,
            int topicPartition, ILoggerFactory loggerFactory)
        {
            this.topicConsumer = topicConsumer;
            this.loggerFactory = loggerFactory;
            this.logPrefix = $"{consumerGroup}-{topicName}{topicPartition}-{streamId}";
            this.streamId = streamId;
            this.logger = this.loggerFactory.CreateLogger<StreamStateManager>();
            this.storageDir = Path.Combine(App.GetStateStorageRootDir(), consumerGroup, topicName, topicPartition.ToString());
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamStateManager"/> class with the specified parameters.
        /// </summary>
        /// <param name="streamId">The ID of the stream.</param>
        /// <param name="stateStorage">The state storage to use.</param>
        /// <param name="loggerFactory">The logger factory used for creating loggers.</param>
        /// <param name="logPrefix">The prefix to be used in log messages.</param>
        public StreamStateManager(string streamId, string consumerGroup, string topicName, int topicPartition,
            ILoggerFactory loggerFactory)
        {
            this.topicConsumer = null;
            this.topicConsumer = topicConsumer;
            this.loggerFactory = loggerFactory;
            this.logPrefix = $"{consumerGroup}-{topicName}-{topicPartition}-{streamId}";
            this.streamId = streamId;
            this.logger = this.loggerFactory.CreateLogger<StreamStateManager>();
            this.storageDir = Path.Combine(App.GetStateStorageRootDir(), consumerGroup, topicName, topicPartition.ToString());
        }
        
        /// <summary>
        /// Deletes all states for the current stream.
        /// </summary>
        public void DeleteStates()
        {
            this.logger.LogTrace("Deleting Stream states for {0}", streamId);
            var storage = GetStreamStorage(this.storageDir, this.streamId);
            storage.Clear();
            this.states.Clear();
        }

        /// <summary>
        /// Deletes the state with the specified name
        /// </summary>
        /// <param name="stateName">The state to delete</param>
        public void DeleteState(string stateName)
        {
            this.logger.LogTrace("Deleting Stream state {0} for {1}", stateName, streamId);
            var storage = GetStateStorage(this.storageDir, this.streamId, stateName);
            storage.Clear();
            this.states.Remove(stateName);
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
                var storage = GetStateStorage(this.storageDir, this.streamId, stateName);
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
                var storage = GetStateStorage(this.storageDir, this.streamId, stateName);
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
                var storage = GetStateStorage(this.storageDir, this.streamId, stateName);
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
                var storage = GetStateStorage(this.storageDir, this.streamId, stateName);
                var state = new StreamScalarState<T>(storage,
                    defaultValueFactory, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }

        /// <summary>
        /// Creates a new application state of given type with automatically managed lifecycle for the stream
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
                };
                return state;
            }
        }
        private static IStateStorage GetStateStorage(string dbDirectory, string streamId, string stateName)
        {
            if (App.GetStateStorageType() == App.StateStorageTypes.RocksDb)
            {
                return RocksDbStorage.GetStateStorage(dbDirectory, streamId, stateName);
            }
            else if (App.GetStateStorageType() == App.StateStorageTypes.InMemory)
            {
                return InMemoryStorage.GetStateStorage(streamId, stateName);
            }
            else
            {
                throw new ArgumentException("Invalid state storage type");
            }
        }
        
        private static IStateStorage GetStreamStorage(string dbDirectory, string streamId)
        {
            if (App.GetStateStorageType() == App.StateStorageTypes.RocksDb)
            {
                return RocksDbStorage.GetStreamStorage(dbDirectory, streamId);
            }
            else if (App.GetStateStorageType() == App.StateStorageTypes.InMemory)
            {
                return InMemoryStorage.GetStreamStorage(streamId);
            }
            else
            {
                throw new ArgumentException("Invalid state storage type");
            }
        }
    }
}