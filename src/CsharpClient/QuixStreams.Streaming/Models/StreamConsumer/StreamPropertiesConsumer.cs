using System;
using System.Collections.Generic;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Represents properties and metadata of the stream.
    /// All changes to these properties are automatically populated to this class.
    /// </summary>
    public class StreamPropertiesConsumer : IDisposable
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPropertiesConsumer"/>
        /// </summary>
        /// <param name="topicConsumer">The topic consumer which owns the stream consumer</param>
        /// <param name="streamConsumer">The Stream consumer which owns this stream event consumer</param>
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
        /// Raised when the stream properties change
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
        /// Gets the list of Stream IDs for the parent streams
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
