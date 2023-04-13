using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
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

        private static bool IsJson(byte[] bytes)
        {
            bool IsObject()
            {
                if (bytes.Length < JsonOpeningCharacter.Length + JsonClosingCharacter.Length) return false;
                for (var index = 0; index < JsonOpeningCharacter.Length; index++)
                {
                    if (bytes[index] != JsonOpeningCharacter[index]) return false;
                }

                for (var index = JsonClosingCharacter.Length - 1; index >= 0; index--)
                {
                    if (bytes[bytes.Length - 1 - index] != JsonClosingCharacter[index]) return false;
                }

                return true;
            }

            bool IsArray()
            {
                if (bytes.Length < ArrayOpeningCharacter.Length + ArrayOpeningCharacter.Length) return false;
                for (var index = 0; index < ArrayOpeningCharacter.Length; index++)
                {
                    if (bytes[index] != ArrayOpeningCharacter[index]) return false;
                }

                for (var index = ArrayOpeningCharacter.Length - 1; index >= 0; index--)
                {
                    if (bytes[bytes.Length - 1 - index] != ArrayClosingCharacter[index]) return false;
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
                var valueStartsAt = -1L;
                var valueEndsAt = -1L;
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

                                    valueStartsAt = (long) reader.Value;
                                    break;
                                case ValueBytesEndPropertyName:
                                    ReadNext(reader);
                                    if (reader.TokenType != JsonToken.Integer)
                                    {
                                        FailSerialization();
                                    }

                                    valueEndsAt = (long) reader.Value;
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

                var valueBytes = new byte[valueEndsAt - valueStartsAt];
                ms.Position = valueStartsAt;
                ms.Read(valueBytes, 0, valueBytes.Length);
                if (!IsJson(valueBytes))
                {
                    var reader = new JsonTextReader(new StreamReader(ms));
                    ms.Position = valueStartsAt;
                    var newValueBytes = reader.ReadAsBytes();
                    valueBytes = newValueBytes;
                }
                return new TransportPackageValue(valueBytes, new CodecBundle(modelKey, codecId), metaData);
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
                        writer.WriteValue(value);
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