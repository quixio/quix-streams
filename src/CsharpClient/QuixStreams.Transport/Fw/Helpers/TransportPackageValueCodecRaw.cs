using System.Linq;
using System.Runtime.Serialization;
using QuixStreams.Transport.Codec;
using QuixStreams.Transport.Fw.Codecs;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Registry;

namespace QuixStreams.Transport.Fw.Helpers
{
    /// <summary>
    /// Codec used to serialize TransportPackageValue
    /// Doesn't inherit from <see cref="ICodec{TContent}"/> because isn't intended for external or generic use
    /// and the interface slightly compicates the implementation for no benefit
    /// </summary>
    internal static class TransportPackageValueCodecRaw
    {
        public static TransportPackageValue Deserialize(byte[] contentBytes)
        {
            var modelKey = ModelKey.WellKnownModelKeys.ByteArray;
            var codec = CodecRegistry.RetrieveCodecs(modelKey).FirstOrDefault() ?? throw new SerializationException($"Failed to serialize '{modelKey}' because there is no codec registered for it.");
            var metaData = MetaData.Empty;
            
            return new TransportPackageValue(contentBytes, new CodecBundle(modelKey, codec.Id),  metaData);
        }
    }
}