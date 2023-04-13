using QuixStreams.Transport.Fw.Helpers;

namespace QuixStreams.Transport.Fw.Models
{
    /// <summary>
    /// Codec type for serializing a <see cref="TransportPackageValue"/>
    /// </summary>
    public enum TransportPackageValueCodecType
    {
        /// <summary>
        /// Binary codec
        /// </summary>
        Binary,
        
        /// <summary>
        /// Json codec
        /// </summary>
        Json
    }
}