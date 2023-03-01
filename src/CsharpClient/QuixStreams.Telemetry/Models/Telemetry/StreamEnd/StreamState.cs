namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Type of StreamEnd closure message
    /// </summary>
    public enum StreamEndType
    {
        /// <summary>
        /// The stream was closed normally
        /// </summary>
        Closed = 0,

        /// <summary>
        /// The stream was aborted by your code for your own reasons
        /// </summary>
        Aborted = 1,

        /// <summary>
        /// The stream was terminated unexpectedly while data was being written
        /// </summary>
        Terminated = 2
    }
}