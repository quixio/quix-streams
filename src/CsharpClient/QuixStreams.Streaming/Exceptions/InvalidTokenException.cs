using System;

namespace QuixStreams.Streaming.Exceptions
{
    /// <summary>
    /// Exception due to an invalid token during the connection setup of a Quix streaming client 
    /// </summary>
    public class InvalidTokenException: Exception
    {
        /// <summary>
        /// Initializes a new instance of <see cref="InvalidTokenException"/>
        /// </summary>
        /// <param name="message">The message</param>
        /// <param name="innerException">Inner exception if it exists</param>
        public InvalidTokenException(string message, Exception innerException = null) : base(message, innerException)
        {
            
        }
    }
}