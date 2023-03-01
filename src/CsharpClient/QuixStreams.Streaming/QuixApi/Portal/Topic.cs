using System;

namespace QuixStreams.Streaming.QuixApi.Portal
{
    /// <summary>
    /// Describes properties of a topic
    /// </summary>
    internal class Topic
    {
        private string id;

        /// <summary>
        /// Used in Quix Streams to connect to the topic
        /// </summary>
        public string Id
        {
            get { return this.id; }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(Id));
                this.id = value;
            }
        }

        /// <summary>
        /// The name of the topic
        /// </summary>
        public string Name { get; set; }

        private string workspaceId;

        /// <summary>
        /// The unique identifier of the workspace
        /// </summary>
        public string WorkspaceId
        {
            get { return this.workspaceId; }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(WorkspaceId));
                this.workspaceId = value.ToLowerInvariant();
            }
        }

        /// <summary>
        /// The status of the topic
        /// </summary>
        public TopicStatus Status { get; set; }
    }

    /// <summary>
    /// The possible statuses of a topic
    /// </summary>
    internal enum TopicStatus
    {
        /// <summary>
        /// The topic is currently being created
        /// </summary>
        Creating,

        /// <summary>
        /// The topic is ready for use
        /// </summary>
        Ready,

        /// <summary>
        /// The topic is currently being deleted
        /// </summary>
        Deleting,

        /// <summary>
        /// The topic has encountered an error
        /// </summary>
        Error
    }

}