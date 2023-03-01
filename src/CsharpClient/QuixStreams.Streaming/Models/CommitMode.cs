namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// The mode for committing packages
    /// </summary>
    public enum CommitMode
    {
        /// <summary>
        /// The default automatic commit strategy
        /// </summary>
        Automatic,
        
        /// <summary>
        /// Automatic commit is disabled
        /// </summary>
        Manual
    }
}