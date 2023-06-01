using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using QuixStreams.Transport.Codec;
using QuixStreams.Transport.Fw.Codecs;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Fw.Helpers
{
    /// <summary>
    /// Codec used to serialize TransportPackageValue
    /// Doesn't inherit from <see cref="ICodec{TContent}"/> because isn't intended for external or generic use
    /// and the interface slightly compicates the implementation for no benefit
    /// </summary>
    internal static class TransportPackageValueCodecJSON
    {
        private const string CodecIdPropertyName = "C";
        private const string ModelKeyPropertyName = "K";
        private const string ValueBytesPropertyName = "V";
        private const string ValueBytesStartPropertyName = "S";
        private const string ValueBytesEndPropertyName = "E";
        private const string MetaDataPropertyName = "M";
        public static readonly byte[] JsonOpeningCharacter = Constants.Utf8NoBOMEncoding.GetBytes(@"{");
        public static readonly byte[] JsonClosingCharacter = Constants.Utf8NoBOMEncoding.GetBytes(@"}");
        public static readonly byte[] ArrayOpeningCharacter = Constants.Utf8NoBOMEncoding.GetBytes(@"[");
        public static readonly byte[] ArrayClosingCharacter = Constants.Utf8NoBOMEncoding.GetBytes(@"]");

        private static bool IsJson(ArraySegment<byte> bytes)
        {
            bool IsObject()
            {
                if (bytes.Count < JsonOpeningCharacter.Length + JsonClosingCharacter.Length) return false;
                for (var index = 0; index < JsonOpeningCharacter.Length; index++)
                {
                    if (bytes.Array[bytes.Offset + index] != JsonOpeningCharacter[index]) return false;
                }

                for (var index = JsonClosingCharacter.Length - 1; index >= 0; index--)
                {
                    if (bytes.Array[bytes.Offset + bytes.Count - 1 - index] != JsonClosingCharacter[index]) return false;
                }

                return true;
            }

            bool IsArray()
            {
                if (bytes.Count < ArrayOpeningCharacter.Length + ArrayOpeningCharacter.Length) return false;
                for (var index = 0; index < ArrayOpeningCharacter.Length; index++)
                {
                    if (bytes.Array[bytes.Offset +index] != ArrayOpeningCharacter[index]) return false;
                }

                for (var index = ArrayOpeningCharacter.Length - 1; index >= 0; index--)
                {
                    if (bytes.Array[bytes.Offset + bytes.Count - 1 - index] != ArrayClosingCharacter[index]) return false;
                }

                return true;
            }

            return IsArray() || IsObject();
        }
        
        public static TransportPackageValue Deserialize(byte[] contentBytes)
        {
            using (var ms = new MemoryStream(contentBytes))
            using (var sr = new StreamReader(ms, Constants.Utf8NoBOMEncoding))
            {
                var codecId = CodecId.WellKnownCodecIds.None;
                var modelKey = ModelKey.WellKnownModelKeys.Default;
                var metaData = MetaData.Empty;
                var valueAvailable = false;
                int valueStartsAt = -1;
                int valueEndsAt = -1;
                using (var reader = new JsonTextReader(sr))
                {
                    reader.CloseInput = false;
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonToken.PropertyName)
                        {
                            switch (reader.Value)
                            {
                                case CodecIdPropertyName:
                                    ReadNext(reader);
                                    if (reader.TokenType != JsonToken.String)
                                    {
                                        FailSerialization();
                                    }

                                    codecId = (string) reader.Value;
                                    break;
                                case ModelKeyPropertyName:
                                    ReadNext(reader);
                                    if (reader.TokenType != JsonToken.String)
                                    {
                                        FailSerialization();
                                    }

                                    modelKey = (string) reader.Value;
                                    break;
                                case MetaDataPropertyName:
                                    metaData = ParseMetaDataJSON(reader);
                                    break;
                                case ValueBytesStartPropertyName:
                                    ReadNext(reader);
                                    if (reader.TokenType != JsonToken.Integer)
                                    {
                                        FailSerialization();
                                    }

                                    valueStartsAt = (int)(long) reader.Value;
                                    break;
                                case ValueBytesEndPropertyName:
                                    ReadNext(reader);
                                    if (reader.TokenType != JsonToken.Integer)
                                    {
                                        FailSerialization();
                                    }

                                    valueEndsAt = (int)(long) reader.Value;
                                    break;
                                case ValueBytesPropertyName:
                                    valueAvailable = true;
                                    reader.Skip();
                                    break;
                            }
                        }
                    }
                }

                if (!valueAvailable || (valueStartsAt == -1) | (valueEndsAt == -1))
                {
                    throw new SerializationException($"Failed to deserialize '{nameof(TransportPackageValue)}' because model value details are not found");
                }

                var content = new ArraySegment<byte>(contentBytes, valueStartsAt, valueEndsAt - valueStartsAt);
                if (!IsJson(content))
                {
                    // +1, -2, because the value is "....", so trimming the leading and trailing "
                    byte[] decodedByteArray =Convert.FromBase64String(Encoding.ASCII.GetString(content.Array, content.Offset + 1 , content.Count -2 )); 

                    content = new ArraySegment<byte>(decodedByteArray);
                }

                return new TransportPackageValue(content, new CodecBundle(modelKey, codecId), metaData);
            }
        }
        

        public static byte[] Serialize(TransportPackageValue transportPackageValue)
        {
            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms, Constants.Utf8NoBOMEncoding))
            {
                using (var writer = new JsonTextWriter(sw)
                {
                    Formatting = Formatting.None // This is extremely important, because this way there is no Byte Order Marker
                })
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(CodecIdPropertyName);
                    writer.WriteValue(transportPackageValue.CodecBundle.CodecId);
                    writer.WritePropertyName(ModelKeyPropertyName);
                    writer.WriteValue(transportPackageValue.CodecBundle.ModelKey);

                    if (transportPackageValue.MetaData != null && transportPackageValue.MetaData.Count > 0)
                    {
                        writer.WritePropertyName(MetaDataPropertyName);
                        writer.WriteRawValue(JsonConvert.SerializeObject(transportPackageValue.MetaData));
                    }

                    writer.WritePropertyName(ValueBytesPropertyName);
                    writer.Flush();
                    var startPosition = ms.Position;
                    var value = transportPackageValue.Value;
                    if (IsJson(value))
                    {
                        var sentData = StringCodec.Instance.Deserialize(value);
                        writer.WriteRawValue(sentData);
                    }
                    else
                    {
                        writer.WriteValue(value.ToArray());
                    }

                    writer.Flush();
                    var endPosition = ms.Position;

                    writer.WritePropertyName(ValueBytesStartPropertyName);
                    writer.WriteValue(startPosition);
                    writer.WritePropertyName(ValueBytesEndPropertyName);
                    writer.WriteValue(endPosition);

                    writer.WriteEnd();
                    writer.Flush();
                }

                return ms.ToArray();
            }
        }
        
        private static MetaData ParseMetaDataJSON(JsonReader reader)
        {
            var dictionary = new Dictionary<string, string>();
            ReadNext(reader);
            if (reader.TokenType != JsonToken.StartObject)
            {
                FailSerialization();
            }

            ReadNext(reader);


            while (reader.TokenType != JsonToken.EndObject)
            {
                switch (reader.TokenType)
                {
                    case JsonToken.PropertyName:
                        var key = (string) reader.Value;
                        var value = reader.ReadAsString();
                        dictionary.Add(key, value);
                        break;
                }

                ReadNext(reader);
            }

            return new MetaData(dictionary);
        }
        
        private static void ReadNext(JsonReader reader)
        {
            if (!reader.Read())
            {
                FailSerialization();
            }
        }

        private static void FailSerialization()
        {
            throw new SerializationException(
                $"Failed to deserialize '{nameof(TransportPackageValue)}' because of unexpected json token");
        }
    }
}