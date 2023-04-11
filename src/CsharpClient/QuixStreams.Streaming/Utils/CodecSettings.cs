using QuixStreams.Telemetry.Models;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.Fw.Models;

namespace QuixStreams.Streaming.Utils
{
    /// <summary>
    /// Global Codec settings for streams.
    /// </summary>
    public static class CodecSettings
    {
        /// <summary>
        /// Sets the codec type to be used by producers and transfer package value serialization 
        /// </summary>
        /// <param name="codecType"></param>
        public static void SetGlobalCodecType(CodecType codecType)
        {
            CodecRegistry.Register(producerCodec: codecType);
            
            if (codecType == CodecType.Protobuf)
            {
                SerializingModifier.PackageCodecType = TransportPackageValueCodecType.Binary;
            }
            else
            {
                SerializingModifier.PackageCodecType = TransportPackageValueCodecType.Json;
            }
        }
    }
}