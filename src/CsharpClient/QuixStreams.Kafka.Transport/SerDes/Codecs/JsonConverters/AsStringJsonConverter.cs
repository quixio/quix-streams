using System;
using Newtonsoft.Json;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs.JsonConverters
{
    /// <summary>
    /// <see cref="JsonConverter"/> de/serializing model as a string
    /// </summary>
    public class AsStringJsonConverter : JsonConverter
    {
        /// <inheritdoc />
        public override bool CanRead => false;

        /// <inheritdoc />
        public override bool CanConvert(Type objectType)
        {
            return true;
        }

        /// <inheritdoc />
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value.ToString());
        }

        /// <inheritdoc />
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return reader.ReadAsString();
        }
    }
}