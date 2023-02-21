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
    internal static class TransportPackageValueCodecBinary
    {
        public static TransportPackageValue Deserialize(byte[] contentBytes){
            var codecId = CodecId.WellKnownCodecIds.None;
            var modelKey = ModelKey.WellKnownModelKeys.Default;
            var metaData = MetaData.Empty;
            byte[] valueBytes = new byte[0];

            using (var ms = new MemoryStream(contentBytes)) {
                using (var reader = new BinaryReader(ms))
                {
                    var codecVersion = reader.ReadByte();
                    codecId = reader.ReadString();
                    modelKey = reader.ReadString();

                    metaData = ParseMetaData(reader);
                    var datalen = reader.ReadInt32();
                    valueBytes = reader.ReadBytes(datalen);

                    return new TransportPackageValue(new Lazy<byte[]>(() => valueBytes), new CodecBundle(modelKey, codecId),
                        metaData);
                }
            }
        }


        private static MetaData ParseMetaData(BinaryReader reader)
        {
            byte codecFormat = reader.ReadByte();
            if (codecFormat != 1)
            {
                throw new SerializationException(
                    $"Unknown format for metadata");
            }

            int count = reader.ReadInt32();
            var dictionary = new Dictionary<string, string>();
            for (var i = 0; i < count; ++i)
            {
                var key = reader.ReadString();
                var value = reader.ReadString();
                dictionary[key] = value;
            }
            return new MetaData(dictionary);
        }


        public static void SerializeMetadata(BinaryWriter writer, MetaData metaData)
        {
            byte codecVersion = 1;
            writer.Write(codecVersion);

            int count = metaData.Count;
            writer.Write(count);
            foreach (var keyValuePair in metaData)
            {
                writer.Write(keyValuePair.Key);
                writer.Write(keyValuePair.Value);
            }
        }

        public static byte[] Serialize(TransportPackageValue transportPackageValue)
        {
            try
            {

            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms, Constants.Utf8NoBOMEncoding))
            {
                using (var writer = new BinaryWriter(ms))
                {
                    byte codecVersion = 1;
                    writer.Write(codecVersion);

                    writer.Write(transportPackageValue.CodecBundle.CodecId);
                    writer.Write(transportPackageValue.CodecBundle.ModelKey);

                    SerializeMetadata(writer, transportPackageValue.MetaData);

                    byte[] value = transportPackageValue.Value.Value;
                    writer.Write(value.Length);
                    writer.Write(value);
                    
                    writer.Flush();
                }

                return ms.ToArray();
            }
            
            }
            catch (Exception e)
            {
                Console.Write(e);
                throw e;
            }

        }
    }
}