using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Manages the states of a stream.
    /// </summary>
    public class StreamStateManager
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly ILoggerFactory loggerFactory;
        private readonly string logPrefix;
        private readonly ILogger logger;

        private readonly object stateLock = new object();
        private readonly Dictionary<string, IStreamState> states = new Dictionary<string, IStreamState>();
        private readonly IStateStorage stateStorage;
        private readonly string streamId;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamStateManager"/> class with the specified parameters.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer used for committing state changes.</param>
        /// <param name="streamId">The ID of the stream.</param>
        /// <param name="stateStorage">The state storage to use.</param>
        /// <param name="loggerFactory">The logger factory used for creating loggers.</param>
        /// <param name="logPrefix">The prefix to be used in log messages.</param>
        internal StreamStateManager(ITopicConsumer topicConsumer, string streamId, IStateStorage stateStorage, ILoggerFactory loggerFactory, string logPrefix)
        {
            this.topicConsumer = topicConsumer;
            this.loggerFactory = loggerFactory;
            this.logPrefix = $"{logPrefix}{streamId}";
            this.logger = this.loggerFactory.CreateLogger<StreamStateManager>();
            this.streamId = streamId;
            this.stateStorage = stateStorage;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamStateManager"/> class with the specified parameters.
        /// </summary>
        /// <param name="streamId">The ID of the stream.</param>
        /// <param name="stateStorage">The state storage to use.</param>
        /// <param name="loggerFactory">The logger factory used for creating loggers.</param>
        /// <param name="logPrefix">The prefix to be used in log messages.</param>
        public StreamStateManager(string streamId, IStateStorage stateStorage, ILoggerFactory loggerFactory, string logPrefix)
        {
            this.topicConsumer = null;
            this.loggerFactory = loggerFactory;
            this.logPrefix = $"{logPrefix}{streamId}";
            this.logger = this.loggerFactory.CreateLogger<StreamStateManager>();
            this.streamId = streamId;
            this.stateStorage = stateStorage;
        }

        /// <summary>
        /// Returns an enumerable collection of all available state names for the current stream.
        /// </summary>
        /// <returns>An enumerable collection of string values representing the names of the stream states.</returns>
        public IEnumerable<string> GetStates()
        {
            return this.stateStorage.GetSubStorages();
        }
        
        /// <summary>
        /// Deletes all states for the current stream.
        /// </summary>
        /// <returns>The number of states that were deleted.</returns>
        public int DeleteStates()
        {
            this.logger.LogTrace("Deleting Stream states for {0}", streamId);
            var count = this.stateStorage.DeleteSubStorages();
            this.states.Clear();
            return count;
        }
        
        /// <summary>
        /// Deletes the state with the specified name
        /// </summary>
        /// <param name="stateName">The state to delete</param>
        /// <returns>Whether the state was deleted</returns>
        public bool DeleteState(string stateName)
        {
            this.logger.LogTrace("Deleting Stream state {0} for {1}", stateName, streamId);
            if (!this.stateStorage.DeleteSubStorage(stateName)) return false;
            this.states.Remove(stateName);
            return true;
        }

        /// <summary>
        /// Creates a new application state of given type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <param name="createState">The function to create the state</param>
        /// <typeparam name="TState">The type of the stream state</typeparam>
        /// <returns>Stream state</returns>
        private TState GetStreamState<TState>(string stateName, Func<string, TState> createState) where TState : IStreamState
        {
            if (this.states.TryGetValue(stateName, out var existingState))
            {
                if (!(existingState is TState castedExistingState))
                {
                    throw new ArgumentException($"{logPrefix}, State '{stateName}' already exists as different stream state type.");
                }
                
                return castedExistingState;
            }
            
            lock (stateLock)
            {
                if (this.states.TryGetValue(stateName, out existingState))
                {
                    if (!(existingState is TState castedExistingState))
                    {
                        throw new ArgumentException($"{logPrefix}, State '{stateName}' already exists as different stream state type.");
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
                var state = new StreamDictionaryState(this.stateStorage.GetOrCreateSubStorage(stateName), new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }
        
        /// <summary>
        /// Creates a new application state of dictionary type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <param name="defaultValueFactory">The value factory for the state when the state has no value for the key</param>
        /// <returns>Dictionary stream state</returns>
        public StreamDictionaryState<T> GetDictionaryState<T>(string stateName, StreamStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            return GetStreamState(stateName, (stateName) =>
            {
                this.logger.LogTrace("Creating Stream state for {0}", stateName);
                var state = new StreamDictionaryState<T>(this.stateStorage.GetOrCreateSubStorage(stateName), defaultValueFactory, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
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
                var state = new StreamScalarState(this.stateStorage.GetOrCreateSubStorage(stateName), new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }
        
        /// <summary>
        /// Creates a new application state of scalar type with automatically managed lifecycle for the stream
        /// </summary>
        /// <param name="stateName">The name of the state</param>
        /// <param name="defaultValueFactory">The value factory for the state when the state has no value for the key</param>
        /// <returns>Dictionary stream state</returns>
        public StreamScalarState<T> GetScalarState<T>(string stateName, StreamStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            return GetStreamState(stateName, (stateName) =>
            {
                this.logger.LogTrace("Creating Stream state for {0}", stateName);
                var state = new StreamScalarState<T>(this.stateStorage.GetOrCreateSubStorage(stateName), defaultValueFactory, new PrefixedLoggerFactory(this.loggerFactory, $"{logPrefix} - {stateName}"));
                return state;
            });
        }
    }
}