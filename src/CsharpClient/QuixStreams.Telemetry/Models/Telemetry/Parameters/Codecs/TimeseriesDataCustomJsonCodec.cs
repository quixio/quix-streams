using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Telemetry.Models.Telemetry.Parameters.Codecs
{
    /// <summary>
    /// TimeseriesData Json Codec implementation
    /// </summary>
    public class TimeseriesDataCustomJsonCodec : Codec<TimeseriesDataRaw>
    {
        private static readonly DefaultJsonCodec<TimeseriesDataRaw> BaseCodec = new DefaultJsonCodec<TimeseriesDataRaw>();
        private static UTF8Encoding UTF8NoBom = new UTF8Encoding(false, false);
        
        /// <inheritdoc />
        public override CodecId Id => BaseCodec.Id; // this is only a serialization codec, still valid JSON

        private Converter JsonConverter = new Converter(JsonSerializer.CreateDefault());

        /// <inheritdoc />
        public override TimeseriesDataRaw Deserialize(byte[] contentBytes)
        {
            using (var memoryStream = new MemoryStream(contentBytes))
            {
                using (var streamReader = new StreamReader(memoryStream))
                {
                    using (var jsonReader = new JsonTextReader(streamReader))
                    {
                        return JsonConverter.ReadJson(jsonReader);
                    }
                }
            }
        }

        /// <inheritdoc />
        public override TimeseriesDataRaw Deserialize(ArraySegment<byte> contentBytes)
        {
            using (var memoryStream = new MemoryStream(contentBytes.Array, contentBytes.Offset, contentBytes.Count))
            {
                using (var streamReader = new StreamReader(memoryStream))
                {
                    using (var jsonReader = new JsonTextReader(streamReader))
                    {
                        return JsonConverter.ReadJson(jsonReader);
                    }
                }
            }
        }



        /// <inheritdoc />
        public override byte[] Serialize(TimeseriesDataRaw obj)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var streamWriter = new StreamWriter(memoryStream, UTF8NoBom, 4096, true))
                {
                    using (var jsonWriter = new JsonTextWriter(streamWriter))
                    {
                        jsonWriter.CloseOutput = false;
                        JsonConverter.WriteJson(jsonWriter, obj);
                    }
                }

                var arr = new byte[memoryStream.Position];
                Array.Copy(memoryStream.ToArray(), arr, memoryStream.Position);
                return arr;
            }
        }

        public class Converter
        {
            private readonly JsonSerializer serializer;

            public Converter(JsonSerializer serializer)
            {
                this.serializer = serializer;
            }

            public TimeseriesDataRaw ReadJson(JsonReader reader)
            {
                long epoch = 0;
                var size = -1;
                long[] timestamps = null;
                Dictionary<string, double?[]> numericValues = null;
                Dictionary<string, string[]> stringValues = null;
                Dictionary<string, byte[][]> binaryValues = null;
                Dictionary<string, string[]> tagValues = null;

                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.PropertyName)
                    {
                        switch (reader.Value)
                        {
                            case "Epoch":
                                reader.Read();
                                epoch = (long)reader.Value;
                                break;
                            case "Timestamps":
                                timestamps = ParseArray(reader, ref size, o => (long)o);
                                break;
                            case "NumericValues":
                                bool optimist = true;
                                numericValues = ParseDict(reader, ref size, o =>
                                {
                                    // optimist
                                    if (optimist)
                                    {
                                        try
                                        {
                                            return (double?)o;
                                        }
                                        catch (InvalidCastException)
                                        {
                                            optimist = false;
                                            return (long?)o;
                                        }
                                    }
                                    // pesssimist
                                    try
                                    {
                                        return (long?)o;
                                    }
                                    catch (InvalidCastException)
                                    {
                                        optimist = true;
                                        return (double?)o;
                                    }
                                });
                                break;
                            case "StringValues":
                                stringValues = ParseDict(reader, ref size, o => (string)o);
                                break;
                            case "BinaryValues":
                                binaryValues = ParseDict(reader, ref size, o => o == null ? null : Convert.FromBase64String((string)o));
                                break;
                            case "TagValues":
                                tagValues = ParseDict(reader, ref size, o => (string)o);
                                break;
                        }
                    }
                }


                return new TimeseriesDataRaw
                {
                    Epoch = epoch,
                    Timestamps = timestamps?.ToArray() ?? Array.Empty<long>(),
                    NumericValues = numericValues ?? new Dictionary<string, double?[]>(),
                    StringValues = stringValues ?? new Dictionary<string, string[]>(),
                    BinaryValues = binaryValues ?? new Dictionary<string, byte[][]>(),
                    TagValues = tagValues ?? new Dictionary<string, string[]>()
                };
            }

            private Dictionary<string, T[]> ParseDict<T>(JsonReader reader, ref int size, Func<object, T> converter)
            {
                var dict = new Dictionary<string, T[]>();
                reader.Read();
                if (reader.TokenType != JsonToken.StartObject)
                    throw new JsonSerializationException("TagValues serialization error");
                while (reader.Read() && reader.TokenType == JsonToken.PropertyName)
                {
                    var parameterName = (string)reader.Value;
                    var values = ParseArray(reader, ref size, converter);
                    dict.Add(parameterName, values);
                }

                return dict;
            }

            private T[] ParseArray<T>(JsonReader reader, ref int size, Func<object, T> converter)
            {
                reader.Read();
                if (reader.TokenType != JsonToken.StartArray)
                    throw new JsonSerializationException($"Expected StartArray, found {reader.TokenType}");

                if (size == -1)
                {
                    var result = new List<T>();
                    while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                    {
                        result.Add(converter(reader.Value));
                    }

                    size = result.Count;

                    return result.ToArray();
                }

                var resultArr = new T[size];
                var index = 0;
                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                    resultArr[index++] = converter(reader.Value);
                return resultArr;
            }

            private Dictionary<string, TValue[]> DeserializeDictionary<TValue>(JToken token, int arrayCapacity)
            {
                var dictionary = new Dictionary<string, TValue[]>(50);

                if (token != null && token.Type == JTokenType.Object)
                {
                    var jObject = (JObject)token;

                    foreach (var property in jObject.Properties())
                    {
                        var key = property.Name;
                        var value = DeserializeArray<TValue>(jObject[key], arrayCapacity);
                        dictionary.Add(key, value);
                    }
                }

                return dictionary;
            }

            private TValue[] DeserializeArray<TValue>(JToken token, int capacity)
            {
                if (token != null && token.Type == JTokenType.Array)
                {
                    var jArray = (JArray)token;
                    var array = new TValue[capacity];

                    for (int i = 0; i < jArray.Count; i++)
                    {
                        array[i] = jArray[i].ToObject<TValue>(serializer);
                    }

                    return array;
                }

                return null;
            }

            public void WriteJson(JsonWriter writer, TimeseriesDataRaw value)
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(value.Epoch));
                writer.WriteValue(value.Epoch);

                writer.WritePropertyName(nameof(value.Timestamps));
                SerializeArray(writer, value.Timestamps);

                if (value.NumericValues?.Count > 0)
                {
                    writer.WritePropertyName(nameof(value.NumericValues));
                    SerializeDictionary(writer, value.NumericValues);
                }

                if (value.StringValues?.Count > 0)
                {
                    writer.WritePropertyName(nameof(value.StringValues));
                    SerializeDictionary(writer, value.StringValues);
                }

                if (value.BinaryValues?.Count > 0)
                {
                    writer.WritePropertyName(nameof(value.BinaryValues));
                    SerializeDictionary(writer, value.BinaryValues);
                }

                if (value.TagValues?.Count > 0)
                {
                    writer.WritePropertyName(nameof(value.TagValues));
                    SerializeDictionary(writer, value.TagValues);
                }

                writer.WriteEndObject();

            }

            private void SerializeDictionary<T>(JsonWriter writer, Dictionary<string, T[]> dict)
            {
                writer.WriteStartObject();

                foreach (var kvp in dict)
                {
                    writer.WritePropertyName(kvp.Key);
                    SerializeArray(writer, kvp.Value);
                }

                writer.WriteEndObject();
            }

            private void SerializeArray<T>(JsonWriter writer, T[] array)
            {
                writer.WriteStartArray();

                for (int i = 0; i < array.Length; i++)
                {
                    var val = array[i];
                    if (val != null) writer.WriteValue(val);
                    else writer.WriteNull();
                }

                writer.WriteEndArray();
            }
        }
    }
}