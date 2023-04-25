using System;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.Models.StreamConsumer;
using QuixStreams.Streaming.States;
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
        /// Gets the consumer for accessing the properties and metadata of the stream.
        /// </summary>
        StreamPropertiesConsumer Properties { get; }

        /// <summary>
        /// Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values.
        /// </summary>
        StreamTimeseriesConsumer Timeseries { get; }

        /// <summary>
        /// Gets the consumer for accessing event related information of the stream such as event definitions and values.
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

        /// <summary>
        /// Gets the manager for the stream states
        /// </summary>
        /// <returns>Stream state manager</returns>
        StreamStateManager GetStateManager();
    }

    /// <summary>
    /// Extensions for IStreamConsumer
    /// </summary>
    public static class IStreamConsumerExtensions
    {
        /// <summary>
        /// Gets the stream state for the specified storage name using the provided default value factory.
        /// </summary>
        /// <typeparam name="T">The type of the stream state value.</typeparam>
        /// <param name="streamConsumer">The stream consumer to get the state for</param>
        /// <param name="storageName">The name of the storage.</param>
        /// <param name="defaultValueFactory">A delegate that creates the default value for the stream state when a previously not set key is accessed.</param>
        /// <returns>The stream state for the specified storage name using the provided default value factory.</returns>
        public static StreamState<T> GetState<T>(this IStreamConsumer streamConsumer, string storageName, StreamStateDefaultValueDelegate<T> defaultValueFactory)
        {
            return streamConsumer.GetStateManager().GetState(storageName, defaultValueFactory);
        }
    }
    
    /// <summary>
    /// Provides data for the PackageReceived event.
    /// </summary>
    public class PackageReceivedEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the PackageReceivedEventArgs class.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer associated with the event.</param>
        /// <param name="consumer">The stream consumer associated with the event.</param>
        /// <param name="package">The stream package that was received.</param>
        public PackageReceivedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer, QuixStreams.Telemetry.Models.StreamPackage package)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
            this.Package = package;
        }
        
        /// <summary>
        /// Gets the topic consumer associated with the event.
        /// </summary>
        public ITopicConsumer TopicConsumer { get; }
        
        /// <summary>
        /// Gets the stream consumer associated with the event.
        /// </summary>
        public IStreamConsumer Stream { get; }
        
        /// <summary>
        /// Gets the stream package that was received.
        /// </summary>
        public QuixStreams.Telemetry.Models.StreamPackage Package { get; }
    }

    /// <summary>
    /// Provides data for the StreamClosed event.
    /// </summary>
    public class StreamClosedEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the StreamClosedEventArgs class.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer associated with the event.</param>
        /// <param name="consumer">The stream consumer associated with the event.</param>
        /// <param name="endType">The mode how the stream was closed.</param>
        public StreamClosedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer, StreamEndType endType)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
            this.EndType = endType;
        }
        
        /// <summary>
        /// Gets the topic consumer associated with the event.
        /// </summary>
        public ITopicConsumer TopicConsumer { get; }
        
        /// <summary>
        /// Gets the stream consumer associated with the event.
        /// </summary>
        public IStreamConsumer Stream { get; }
        
        /// <summary>
        /// Gets the mode how the stream was closed.
        /// </summary>
        public StreamEndType EndType { get; }
    }
}