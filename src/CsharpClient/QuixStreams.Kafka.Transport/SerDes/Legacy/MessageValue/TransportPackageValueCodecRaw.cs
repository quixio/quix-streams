using System.Linq;
using System.Runtime.Serialization;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue
{
    /// <summary>
    /// LEGACY Serialization for Transport Package Value when the data used to be encoded inside the message's value
    /// Codec used to serialize TransportPackageValue
    /// Doesn't inherit from <see cref="ICodec"/> because isn't intended for external or generic use
    /// and the interface slightly compicates the implementation for no benefit
    /// </summary>
    internal static class TransportPackageValueCodecRaw
    {
        public static TransportPackageValue Deserialize(byte[] contentBytes)
        {
            var modelKey = ModelKey.WellKnownModelKeys.ByteArray;
            var codec = CodecRegistry.RetrieveCodecs(modelKey).FirstOrDefault() ?? throw new SerializationException($"Failed to serialize '{modelKey}' because there is no codec registered for it.");
            
            return new TransportPackageValue(contentBytes, new CodecBundle(modelKey, codec.Id));
        }
    }
}