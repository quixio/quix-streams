using System;
using Newtonsoft.Json;
using QuixStreams.Streaming.QuixApi.Portal;

namespace QuixStreams.Streaming.QuixApi
{
    internal class WorkspaceBrokerTypeJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value.ToString());
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.String)
            {
                string value = reader.Value.ToString();
                if (Enum.TryParse(value, out WorkspaceBrokerType result))
                {
                    return result;
                }
            }

            return WorkspaceBrokerType.Unknown;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(WorkspaceBrokerType);
        }
    }
}