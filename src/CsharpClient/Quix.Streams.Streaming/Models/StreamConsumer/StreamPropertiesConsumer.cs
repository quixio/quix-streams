using System;
using System.Collections.Generic;
using Quix.Streams.Telemetry.Models;

namespace Quix.Streams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Properties and Metadata of the stream.
    /// All the changes of these properties are populated to this class automatically
    /// </summary>
    public class StreamPropertiesConsumer : IDisposable
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPropertiesConsumer"/>
        /// </summary>
        /// <param name="topicConsumer">The topic the stream to what this reader belongs to</param>
        /// <param name="streamConsumer">Stream reader owner</param>
        internal StreamPropertiesConsumer(ITopicConsumer topicConsumer, IStreamConsumerInternal streamConsumer)
        {
            this.topicConsumer = topicConsumer;
            this.streamConsumer = streamConsumer;

            this.Metadata = new Dictionary<string, string>();
            this.Parents = new List<string>();

            this.streamConsumer.OnStreamPropertiesChanged += OnStreamPropertiesChangedEventHandler;

        }

        private void OnStreamPropertiesChangedEventHandler(IStreamConsumer sender, StreamProperties streamProperties)
        {
            this.Name = streamProperties.Name;
            this.Location = streamProperties.Location;
            this.TimeOfRecording = streamProperties.TimeOfRecording;
            this.Metadata = streamProperties.Metadata;
            this.Parents = streamProperties.Parents;

            this.OnChanged?.Invoke(this, new StreamPropertiesChangedEventArgs(this.topicConsumer, sender));
        }

        /// <summary>
        /// Raised when the stream properties have changed
        /// </summary>
        public event EventHandler<StreamPropertiesChangedEventArgs> OnChanged;

        /// <summary>
        /// Gets the name of the stream
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the location of the stream
        /// </summary>
        public string Location { get; private set; }

        /// <summary>
        /// Gets the datetime of the recording
        /// </summary>
        public DateTime? TimeOfRecording { get; private set; }

        /// <summary>
        /// Gets the metadata of the stream
        /// </summary>
        public Dictionary<string, string> Metadata { get; private set; }

        /// <summary>
        /// List of Stream Ids of the Parent streams
        /// </summary>
        public List<string> Parents { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamConsumer.OnStreamPropertiesChanged -= OnStreamPropertiesChangedEventHandler;
        }
    }

    public class StreamPropertiesChangedEventArgs
    {
        public StreamPropertiesChangedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
        }

        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
    }
}
