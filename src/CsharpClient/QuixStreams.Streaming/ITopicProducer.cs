using System;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Interface to produce outgoing streams
    /// </summary>
    public interface ITopicProducer : IDisposable
    {
        /// <summary>
        /// Creates a new stream and returns the related stream producer to operate it.
        /// </summary>
        /// <returns>Stream producer to allow the stream to push data to the platform</returns>
        IStreamProducer CreateStream();

        /// <summary>
        /// Creates a new stream and returns the related stream producer to operate it.
        /// </summary>
        /// <param name="streamId">Stream Id of the created stream</param>
        /// <returns>Stream producer to allow the stream to push data to the platform</returns>
        IStreamProducer CreateStream(string streamId);

        /// <summary>
        /// Retrieves a stream that was previously created by this instance, if the stream is not closed.
        /// </summary>
        /// <param name="streamId">The Id of the stream</param>
        /// <returns>Stream producer to allow the stream to push data to the platform or null if not found.</returns>
        IStreamProducer GetStream(string streamId);

        /// <summary>
        /// Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream. 
        /// </summary>
        /// <param name="streamId">The Id of the stream you want to get or create</param>
        /// <param name="onStreamCreated">Callback executed when a new Stream is created in the topic producer because it doesn't exist.</param>
        /// <returns>Stream producer to allow the stream to push data to the platform.</returns>
        IStreamProducer GetOrCreateStream(string streamId, Action<IStreamProducer> onStreamCreated = null);

        /// <summary>
        /// Flushes pending data to the broker
        /// </summary>
        void Flush();
        
        /// <summary>
        /// Raised when the resource finished disposing
        /// </summary>
        public event EventHandler OnDisposed;
    }
}