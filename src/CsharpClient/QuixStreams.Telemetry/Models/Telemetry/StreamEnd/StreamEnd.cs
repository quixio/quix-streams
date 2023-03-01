namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Stream End message model.
    /// Indicates that the Stream has been closed, aborted or terminated.
    /// </summary>
    public class StreamEnd
    {
        /// <summary>
        /// Type of closure message
        /// </summary>
        public StreamEndType StreamEndType { get; set; } = StreamEndType.Closed;
    }
}