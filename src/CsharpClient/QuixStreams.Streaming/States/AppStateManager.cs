using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;
using QuixStreams;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Manages the states of a app.
    /// </summary>
    public class AppStateManager
    {
        private readonly ILoggerFactory loggerFactory;
        
        private readonly ConcurrentDictionary<string, TopicStateManager> topicStateManagers = new ConcurrentDictionary<string, TopicStateManager>();

        private readonly ILogger<AppStateManager> logger;
        private readonly IStateStorage storage;


        /// <summary>
        /// Initializes a new instance of the AppStateManager class.
        /// </summary>
        /// <param name="storage">The state storage the use for the application state</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        public AppStateManager(IStateStorage storage, ILoggerFactory loggerFactory = null)
        {
            this.loggerFactory = loggerFactory ?? QuixStreams.Logging.Factory;
            this.storage = storage;
            this.logger = this.loggerFactory.CreateLogger<AppStateManager>();
        }

        /// <summary>
        /// Returns an enumerable collection of all available topic states for the current app.
        /// </summary>
        /// <returns>An enumerable collection of string values representing the topic state names.</returns>
        public IEnumerable<string> GetTopicStates()
        {
            return this.storage.GetSubStorages();
        }
        
        /// <summary>
        /// Deletes all topic states for the current app.
        /// </summary>
        /// <returns>The number of topic states that were deleted.</returns>
        public int DeleteTopicStates()
        {
            this.logger.LogTrace("Deleting topic states");
            var count = this.storage.DeleteSubStorages();
            this.topicStateManagers.Clear();
            return count;
        }
        
        /// <summary>
        /// Deletes the topic state with the specified name
        /// </summary>
        /// <returns>Whether the topic state was deleted</returns>
        public bool DeleteTopicState(string topicName)
        {
            this.logger.LogTrace("Deleting topic states for {0}", topicName);
            if (!this.storage.DeleteSubStorage(GetSubStorageName(topicName))) return false;
            this.topicStateManagers.TryRemove(topicName, out _);
            return true;
        }

        /// <summary>
        /// Returns the sub storage name with correct prefix
        /// </summary>
        /// <param name="topicName">The topic name to prefix</param>
        /// <returns>The prefixed topic name</returns>
        private string GetSubStorageName(string topicName)
        {
            return topicName.Replace(Path.PathSeparator, '_').Replace(Path.DirectorySeparatorChar, '_').Replace(Path.VolumeSeparatorChar, '_').Replace(Path.AltDirectorySeparatorChar, '_');
        }

        /// <summary>
        /// Gets an instance of the <see cref="TopicStateManager"/> class for the specified <paramref name="topicName"/>.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer to create the state manager for</param>
        /// <param name="topicName">The topic name</param>
        /// <returns>The newly created <see cref="TopicStateManager"/> instance.</returns>
        internal TopicStateManager GetTopicStateManager(ITopicConsumer topicConsumer, string topicName)
        {
            return this.topicStateManagers.GetOrAdd(topicName, key =>
            {
                this.logger.LogTrace("Creating Topic state manager for {0}", key);
                return new TopicStateManager(topicConsumer, key,
                        this.storage.GetOrCreateSubStorage(GetSubStorageName(key)), this.loggerFactory);
            });
        }
        
        /// <summary>
        /// Gets an instance of the <see cref="TopicStateManager"/> class for the specified <paramref name="topicName"/>.
        /// </summary>
        /// <param name="topicName">The topic name</param>
        /// <returns>The newly created <see cref="TopicStateManager"/> instance.</returns>
        public TopicStateManager GetTopicStateManager(string topicName)
        {
            return this.topicStateManagers.GetOrAdd(topicName, key =>
            {
                this.logger.LogTrace("Creating Topic state manager for {0}", key);
                return new TopicStateManager(key, this.storage.GetOrCreateSubStorage(GetSubStorageName(key)),
                        this.loggerFactory);
            });
        }
    }
}