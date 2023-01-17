using System;

namespace Quix.Sdk.Streaming.Raw
{
    /// <summary>
    /// Interface to write raw messages into a Topic (capable to write non-sdk messages)
    /// </summary>
    public interface IRawOutputTopic : IDisposable
    {
        /// <summary>
        /// Write data to the topic
        /// </summary>
        public void Write(RawMessage data);
        
        /// <summary>
        /// Raised when the resource is disposed
        /// </summary>
        public event EventHandler OnDisposed;
    }
}