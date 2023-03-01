using System;

namespace QuixStreams.Streaming.Exceptions
{
    /// <summary>
    /// Invalid operation due to the stream being closed already
    /// </summary>
    public class StreamClosedException : InvalidOperationException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="StreamClosedException"/>
        /// </summary>
        /// <param name="message">The message</param>
        public StreamClosedException(string message) : base(message)
        {
            
        }
    }
}