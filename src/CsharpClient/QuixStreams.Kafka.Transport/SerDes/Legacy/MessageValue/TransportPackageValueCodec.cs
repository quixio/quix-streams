using System;
using System.IO;
using System.Runtime.Serialization;
using QuixStreams.Kafka.Transport.SerDes.Codecs;

namespace QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue
{
    /// <summary>
    /// LEGACY Serialization for Transport Package's value when the data used to be encoded inside the message's value
    /// Codec used to serialize TransportPackageValue
    /// Doesn't inherit from <see cref="ICodec"/> because isn't intended for external or generic use
    /// and the interface slightly complicates the implementation for no benefit
    /// </summary>
    internal static class TransportPackageValueCodec
    {
        //Definition of the supported protocols ( defined in the first byte of the packet )
        private static readonly byte PROTOCOL_ID_BYTE = 0x01;
        private static readonly byte PROTOCOL_ID_JSON = TransportPackageValueCodecJSON.JsonOpeningCharacter[0];
        
        public static TransportPackageValue Deserialize(byte[] contentBytes)
        {
            if (contentBytes.Length == 0)
                throw new SerializationException("Failed to deserialize - the packet has length == 0");
            
            try
            {
                var protocolId = contentBytes[0]; // first character is { >> backward compatibility function
                
                if (protocolId == PROTOCOL_ID_BYTE)
                {
                    return TransportPackageValueCodecBinary.Deserialize(contentBytes);
                }

                if (protocolId == PROTOCOL_ID_JSON)
                {
                    return TransportPackageValueCodecJSON.Deserialize(contentBytes);
                }
            }
            catch (Exception ex) when (ex is SerializationException || ex is EndOfStreamException)
            {
                // It is possible that the message starts with PROTOCOL_ID_BYTE or PROTOCOL_ID_JSON but is actually a non-quix message, so we try to deserialize it as a raw message
                return TransportPackageValueCodecRaw.Deserialize(contentBytes);
            }
                
            return TransportPackageValueCodecRaw.Deserialize(contentBytes);
        }

        public static byte[] Serialize(TransportPackageValue transportPackageValue, TransportPackageValueCodecType valueCodecType)
        {
            if (valueCodecType == TransportPackageValueCodecType.Json) return TransportPackageValueCodecJSON.Serialize(transportPackageValue);
            if (valueCodecType == TransportPackageValueCodecType.Binary) return TransportPackageValueCodecBinary.Serialize(transportPackageValue);
            throw new NotImplementedException($"Serialization for {valueCodecType} is not implemented");
        }
    }
}