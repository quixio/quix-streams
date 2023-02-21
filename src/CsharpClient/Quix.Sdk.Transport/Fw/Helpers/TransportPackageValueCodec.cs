using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using Quix.Sdk.Transport.Codec;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.IO;
using Newtonsoft.Json;

namespace Quix.Sdk.Transport.Fw.Helpers
{
    /// <summary>
    /// Codec used to serialize TransportPackageValue
    /// Doesn't inherit from <see cref="ICodec{TransportPackageValue}"/> because isn't intended for external or generic use
    /// and the interface slightly compicates the implementation for no benefit
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
                //first character is { >> backward compatibility function
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

        public static byte[] Serialize(TransportPackageValue transportPackageValue)
        {
            //we support only the serialization in the binary format
            return TransportPackageValueCodecBinary.Serialize(transportPackageValue);
        }
    }
}