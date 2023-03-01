namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// The severity of the event
    /// </summary>
    public enum EventLevel
    {
        /// <summary>
        /// Trace level
        /// </summary>
        Trace = 0,

        /// <summary>
        /// Debug level
        /// </summary>
        Debug = 1,

        /// <summary>
        /// Information level
        /// </summary>
        Information  = 2,

        /// <summary>
        /// Warning level
        /// </summary>
        Warning = 3,

        /// <summary>
        /// Error level
        /// </summary>
        Error = 4,

        /// <summary>
        /// Critical level
        /// </summary>
        Critical = 5,
    }
}