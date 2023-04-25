using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Manages the states of a topic.
    /// </summary>
    public class TopicStateManager
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly string topicName;
        private readonly IStateStorage stateStorage;
        private readonly ILoggerFactory loggerFactory;

        private readonly object stateLock = new object();
        private readonly Dictionary<string, object> states = new Dictionary<string, object>();
        
        private readonly ConcurrentDictionary<string, StreamStateManager> streamStateManagers = new ConcurrentDictionary<string, StreamStateManager>();

        private readonly ILogger<TopicStateManager> logger;

        /// <summary>
        /// Initializes a new instance of the TopicStateManager class.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer this manager is for.</param>
        /// <param name="topicName">The name of the topic</param>
        /// <param name="stateStorage">The state storage to use</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal TopicStateManager(ITopicConsumer topicConsumer, string topicName, IStateStorage stateStorage, ILoggerFactory loggerFactory)
        {
            this.topicConsumer = topicConsumer;
            this.topicName = topicName;
            this.stateStorage = stateStorage;
            this.loggerFactory = loggerFactory;
            this.logger = this.loggerFactory.CreateLogger<TopicStateManager>();
        }
        
        /// <summary>
        /// Initializes a new instance of the TopicStateManager class.
        /// </summary>
        /// <param name="topicName">The name of the topic</param>
        /// <param name="stateStorage">The state storage to use</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal TopicStateManager(string topicName, IStateStorage stateStorage, ILoggerFactory loggerFactory)
        {
            this.topicConsumer = null;
            this.topicName = topicName;
            this.stateStorage = stateStorage;
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
            var state = new TopicState<T>(this.stateStorage, defaultValueFactory, Logging.CreatePrefixedFactory($"{this.topicName} - {nameOfState}"));
            return state;
        }

        /// <summary>
        /// Returns an enumerable collection of all available stream states for the current topic. 
        /// </summary>
        /// <returns>An enumerable collection of string values representing the stream state names.</returns>
        public IEnumerable<string> GetStreamStates()
        {
            return this.stateStorage.GetSubStorages();
        }
        
        /// <summary>
        /// Deletes all stream states for the current topic.
        /// </summary>
        /// <returns>The number of stream states that were deleted.</returns>
        public int DeleteStreamStates()
        {
            var count = this.stateStorage.DeleteSubStorages();
            this.streamStateManagers.Clear();
            return count;        
        }
        
        /// <summary>
        /// Deletes the stream state with the specified name
        /// </summary>
        /// <returns>Whether the stream state was deleted</returns>
        public bool DeleteStreamState(string streamId)
        {
            if (!this.stateStorage.DeleteSubStorage(streamId)) return false;
            this.streamStateManagers.TryRemove(streamId, out _);
            return true;
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
        /// Gets an instance of the <see cref="StreamStateManager"/> class for the specified <paramref name="streamId"/>.
        /// </summary>
        /// <param name="streamId">The ID of the stream.</param>
        /// <returns>The newly created <see cref="StreamStateManager"/> instance.</returns>
        public StreamStateManager GetStreamStateManager(string streamId)
        {
            return this.streamStateManagers.GetOrAdd(streamId, 
                key => new StreamStateManager(this.topicConsumer, streamId, this.stateStorage.GetOrCreateSubStorage(streamId), this.loggerFactory, this.topicName + " "));
        }
    }
}