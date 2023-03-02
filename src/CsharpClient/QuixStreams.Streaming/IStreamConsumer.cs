using System;
using QuixStreams.Streaming.Models.StreamConsumer;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Stream reader interface. Stands for a new stream read from the platform.
    /// Allows to read the stream data received from a topic.
    /// </summary>
    public interface IStreamConsumer
    {
        /// <summary>
        /// Gets the stream Id of the stream.
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// Gets the consumer for accessing the properties and metadata of the stream
        /// </summary>
        StreamPropertiesConsumer Properties { get; }

        /// <summary>
        /// Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values 
        /// </summary>
        StreamTimeseriesConsumer Timeseries { get; }

        /// <summary>
        /// Gets the consumer for accessing event related information of the stream such as event definitions and values 
        /// </summary>
        StreamEventsConsumer Events { get; }

        /// <summary>
        /// Event raised when a stream package has been received.
        /// </summary>
        event EventHandler<PackageReceivedEventArgs> OnPackageReceived;

        /// <summary>
        /// Event raised when the stream has closed.
        /// </summary>
        event EventHandler<StreamClosedEventArgs> OnStreamClosed;
    }
    
    public class PackageReceivedEventArgs
    {
        public PackageReceivedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer, QuixStreams.Telemetry.Models.StreamPackage package)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
            this.Package = package;
        }
        
        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
        public QuixStreams.Telemetry.Models.StreamPackage Package { get; }
    }

    public class StreamClosedEventArgs
    {
        public StreamClosedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer, StreamEndType endType)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
            this.EndType = endType;
        }
        
        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
        public StreamEndType EndType { get; }
    }
}