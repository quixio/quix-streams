namespace QuixStreams.Streaming.QuixApi.Portal
{
    /// <summary>
    /// Model describing the exception which occurred while executing a request
    /// </summary>
    internal class PortalException
    {
        /// <summary>
        /// The exception message
        /// </summary>
        public string Message { get; set; }
        
        /// <summary>
        /// The correlation id used to reference this exception
        /// </summary>
        public string CorrelationId { get; set; }
    }
}