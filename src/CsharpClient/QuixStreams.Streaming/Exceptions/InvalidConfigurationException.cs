using System;

namespace QuixStreams.Streaming.Exceptions
{
    /// <summary>
    /// Exception due to an invalid configuration during the connection setup of a Quix streaming client 
    /// </summary>
    public class InvalidConfigurationException: Exception
    {
        /// <summary>
        /// Initializes a new instance of <see cref="InvalidConfigurationException"/>
        /// </summary>
        /// <param name="message">The message</param>
        /// <param name="innerException">Inner exception if it exists</param>
        public InvalidConfigurationException(string message, Exception innerException = null) : base(message, innerException)
        {
            
        }
    }
}