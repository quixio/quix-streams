using System;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming.Models.StreamConsumer;

namespace Quix.Sdk.Streaming
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
        /// Gets the reader for accessing the properties and metadata of the stream
        /// </summary>
        StreamPropertiesConsumer Properties { get; }

        /// <summary>
        /// Gets the reader for accessing parameter related information of the stream such as definitions and parameter values 
        /// </summary>
        StreamParametersConsumer Parameters { get; }

        /// <summary>
        /// Gets the reader for accessing event related information of the stream such as definitions and event values 
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
        public PackageReceivedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer, Process.Models.StreamPackage package)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
            this.Package = package;
        }
        
        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
        public Process.Models.StreamPackage Package { get; }
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