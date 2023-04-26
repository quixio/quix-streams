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
        
        private static string StreamPrefix = "_S_";
        private static string StreamRegexPattern = "^_S_";

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
        /// Returns an enumerable collection of all available stream states for the current topic. 
        /// </summary>
        /// <returns>An enumerable collection of string values representing the stream state names.</returns>
        public IEnumerable<string> GetStreamStates()
        {
            return this.stateStorage.GetSubStorages(StreamRegexPattern).Select(y=> y.Substring(StreamPrefix.Length));
        }
        
        /// <summary>
        /// Deletes all stream states for the current topic.
        /// </summary>
        /// <returns>The number of stream states that were deleted.</returns>
        public int DeleteStreamStates()
        {
            var count = this.stateStorage.DeleteSubStorages(StreamRegexPattern);
            this.streamStateManagers.Clear();
            return count;        
        }
        
        /// <summary>
        /// Deletes the stream state with the specified name
        /// </summary>
        /// <returns>Whether the stream state was deleted</returns>
        public bool DeleteStreamState(string streamId)
        {
            if (!this.stateStorage.DeleteSubStorage(GetSubStorageName(streamId))) return false;
            this.streamStateManagers.TryRemove(streamId, out _);
            return true;
        }
        
        /// <summary>
        /// Returns the sub storage name with correct prefix
        /// </summary>
        /// <param name="streamId">The stream id to prefix</param>
        /// <returns>The prefixed stream id</returns>
        private string GetSubStorageName(string streamId)
        {
            return $"{StreamPrefix}{streamId}";
        }
        
        /// <summary>
        /// Gets an instance of the <see cref="StreamStateManager"/> class for the specified <paramref name="streamId"/>.
        /// </summary>
        /// <param name="streamId">The ID of the stream.</param>
        /// <returns>The newly created <see cref="StreamStateManager"/> instance.</returns>
        public StreamStateManager GetStreamStateManager(string streamId)
        {
            return this.streamStateManagers.GetOrAdd(streamId, 
                key => new StreamStateManager(this.topicConsumer, key, this.stateStorage.GetOrCreateSubStorage(GetSubStorageName(key)), this.loggerFactory, this.topicName + " "));
        }
    }
}