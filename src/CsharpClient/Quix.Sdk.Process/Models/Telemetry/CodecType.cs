using Quix.Sdk.Transport.Fw.Codecs;

namespace Quix.Sdk.Process.Models
{
    /// <summary>
    /// Codecs available in the library
    /// </summary>
    public enum CodecType
    {
        /// <summary>
        /// Json codecs using <see cref="DefaultJsonCodec"/>
        /// </summary>
        Json,
        
        /// <summary>
        /// Improved Json codecs where the resulting JSON might not be very human friendly, but still in JSON Format
        /// </summary>
        ImprovedJson,
        
        /// <summary>
        /// Protocol buffer format
        /// </summary>
        Protobuf,
    }
}