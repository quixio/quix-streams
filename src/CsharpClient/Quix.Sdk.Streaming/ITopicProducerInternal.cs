using System;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Interface for Output topic internal methods
    /// </summary>
    internal interface ITopicProducerInternal : ITopicProducer
    {
        /// <summary>
        /// Removes a stream from the internal list of streams
        /// </summary>
        void RemoveStream(string streamId);
    }
}