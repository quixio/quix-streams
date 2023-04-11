using QuixStreams.Transport.Fw.Codecs;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Codecs available for serialization. 
    /// </summary>
    public enum CodecType
    {
        /// <summary>
        /// Json codecs using <see cref="DefaultJsonCodec"/>
        /// </summary>
        Json = 0,
        
        /// <summary>
        /// Improved Json codecs where the resulting JSON might not be very human friendly, but still in JSON Format
        /// </summary>
        HumanReadableSemiJsonWithBetterPerformance = 1,
        
        /// <summary>
        /// Protocol buffer format
        /// </summary>
        Protobuf = 2,
    }
}