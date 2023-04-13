using System;
using System.Runtime.Serialization;
using QuixStreams.Transport.Codec;
using QuixStreams.Transport.Fw.Models;

namespace QuixStreams.Transport.Fw.Helpers
{
    /// <summary>
    /// Codec used to serialize TransportPackageValue
    /// Doesn't inherit from <see cref="ICodec{TContent}"/> because isn't intended for external or generic use
    /// and the interface slightly complicates the implementation for no benefit
    /// </summary>
    internal static class TransportPackageValueCodec
    {
        //Definition of the supported protocols ( defined in the first byte of the packet )
        private static readonly byte PROTOCOL_ID_BYTE = 0x01;
        private static readonly byte PROTOCOL_ID_JSON = TransportPackageValueCodecJSON.JsonOpeningCharacter[0];
        
        public static TransportPackageValue Deserialize(byte[] contentBytes)
        {
            if (contentBytes.Length > 0)
            {
                var protocolId = contentBytes[0];
                // first character is { >> backward compatibility function
                if (protocolId == PROTOCOL_ID_BYTE)
                {
                    return TransportPackageValueCodecBinary.Deserialize(contentBytes);
                }
                else if (protocolId == PROTOCOL_ID_JSON)
                {
                    return TransportPackageValueCodecJSON.Deserialize(contentBytes);
                }

                throw new SerializationException(
                    $"Failed to deserialize - the unknown protocol id '{(int) protocolId}'");
            }

            throw new SerializationException($"Failed to deserialize - the packet does length == 0");
        }

        public static byte[] Serialize(TransportPackageValue transportPackageValue, TransportPackageValueCodecType codecType)
        {
            return codecType switch {
                TransportPackageValueCodecType.Json => TransportPackageValueCodecJSON.Serialize(transportPackageValue),
                TransportPackageValueCodecType.Binary => TransportPackageValueCodecBinary.Serialize(transportPackageValue),
                _ => throw new NotImplementedException($"Serialization for {codecType} is not implemented")
            };
        }
    }
}