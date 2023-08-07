using System;
using System.IO;
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
    internal static class TransportPackageValueCodecBinary
    {
        public static TransportPackageValue Deserialize(byte[] contentBytes)
        {

            using (var ms = new MemoryStream(contentBytes))
            {
                using (var reader = new BinaryReader(ms))
                {
                    var codecVersion = reader.ReadByte();
                    var codecId = reader.ReadString();
                    var modelKey = reader.ReadString();

                    SkipMetaData(reader);
                    var dataLen = reader.ReadInt32();
                    var valueBytes = reader.ReadBytes(dataLen);

                    return new TransportPackageValue(valueBytes, new CodecBundle(modelKey, codecId));
                }
            }
        }


        private static void SkipMetaData(BinaryReader reader)
        {
            // Legacy, we terminated Metadata, but left it here for backward compatible serialization purposes as reader needs to be advanced

            byte codecFormat = reader.ReadByte();
            if (codecFormat != 1)
            {
                throw new SerializationException(
                    $"Unknown format for metadata");
            }

            int count = reader.ReadInt32();
            for (var i = 0; i < count; ++i)
            {
                var key = reader.ReadString();
                var value = reader.ReadString();
            }
        }


        private static void SerializeMetadata(BinaryWriter writer)
        {
            // Legacy, we terminated Metadata, but left it here for backward compatible serialization purposes
            byte codecVersion = 1;
            writer.Write(codecVersion);

            int count = 0;
            writer.Write(count);
        }

        public static byte[] Serialize(TransportPackageValue transportPackageValue)
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    byte codecVersion = 1;
                    writer.Write(codecVersion);

                    writer.Write(transportPackageValue.CodecBundle.CodecId);
                    writer.Write(transportPackageValue.CodecBundle.ModelKey);

                    SerializeMetadata(writer);

                    var value = transportPackageValue.Value;
                    writer.Write(value.Count);
                    writer.Write(value.Array, value.Offset, value.Count);

                    writer.Flush();
                }

                return ms.ToArray();
            }
        }
    }
}