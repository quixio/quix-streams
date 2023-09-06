namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Codecs available for serialization and deserialization of streams. 
    /// </summary>
    public enum CodecType
    {
        /// <summary>
        /// Json codecs using <see cref="DefaultJsonCodec"/>
        /// </summary>
        Json = 0,
        
        /// <summary>
        /// Improved Json codecs where the resulting JSON might not be very human friendly, but is more lightweight.
        /// </summary>
        CompactJsonForBetterPerformance = 1,
        
        /// <summary>
        /// Protocol buffer format
        /// </summary>
        Protobuf = 2,
    }
}