using System;
using System.Runtime.Serialization;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// Raised when a codec is missing
    /// </summary>
    public class MissingCodecException : SerializationException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="MissingCodecException"/> with a message 
        /// </summary>
        public MissingCodecException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="MissingCodecException"/> with a message and inner exception
        /// </summary>
        public MissingCodecException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}