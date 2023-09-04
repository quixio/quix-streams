using System;
using System.IO;
using Newtonsoft.Json;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs
{
    /// <summary>
    /// Default JSON codec for any type.
    /// </summary>
    /// <typeparam name="TContent"></typeparam>
    public class DefaultJsonCodec<TContent> : Codec<TContent>
    {
        /// <inheritdoc />
        public override CodecId Id => CodecId.WellKnownCodecIds.DefaultTypedJsonCodec;

        private readonly JsonSerializer serializer;

        /// <summary>
        /// Initializes a new instance of <see cref="DefaultJsonCodec{TContent}"/>
        /// </summary>
        public DefaultJsonCodec()
        {
            serializer = JsonSerializer.Create();
            serializer.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
            serializer.DefaultValueHandling = DefaultValueHandling.Ignore;
        }


        /// <inheritdoc />
        public override TContent Deserialize(byte[] contentBytes)
        {
            using (var stream = new MemoryStream(contentBytes))
            using (var reader = new StreamReader(stream, Constants.Utf8NoBOMEncoding))
            {
                return (TContent) serializer.Deserialize(reader, typeof(TContent));
            }
        }
        
        /// <inheritdoc />
        public override TContent Deserialize(ArraySegment<byte> contentBytes)
        {
            using (var stream = new MemoryStream(contentBytes.Array, contentBytes.Offset, contentBytes.Count))
            using (var reader = new StreamReader(stream, Constants.Utf8NoBOMEncoding))
            {
                return (TContent) serializer.Deserialize(reader, typeof(TContent));
            }
        }

        /// <inheritdoc />
        public override byte[] Serialize(TContent obj)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream, Constants.Utf8NoBOMEncoding))
            {
                serializer.Serialize(writer, obj);
                writer.Flush();
                return stream.ToArray();
            }
        }
    }

    /// <summary>
    /// Default Json codec
    /// </summary>
    public class DefaultJsonCodec : ICodec
    {
        /// <summary>
        /// Static instance of Default Json codec
        /// </summary>
        public static readonly DefaultJsonCodec Instance = new DefaultJsonCodec();
        
        private readonly JsonSerializer serializer;

        private DefaultJsonCodec()
        {
            serializer = JsonSerializer.Create();
            serializer.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
            serializer.DefaultValueHandling = DefaultValueHandling.Ignore;
        }

        /// <inheritdoc />
        public CodecId Id => CodecId.WellKnownCodecIds.DefaultJsonCodec;

        /// <inheritdoc />
        public bool TrySerialize(object obj, out byte[] serialized)
        {
            serialized = null;
            try
            {
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream, Constants.Utf8NoBOMEncoding))
                {
                    serializer.Serialize(writer, obj);
                    writer.Flush();
                    serialized = stream.ToArray();
                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        /// <inheritdoc />
        public bool TryDeserialize(byte[] contentBytes, out object content)
        {
            content = null;
            try
            {
                using (var stream = new MemoryStream(contentBytes))
                using (var reader = new StreamReader(stream, Constants.Utf8NoBOMEncoding))
                {
                    content = serializer.Deserialize(reader, typeof(object));
                    return content != null;
                }
            }
            catch
            {
                return false;
            }
        }

        /// <inheritdoc />
        public bool TryDeserialize(ArraySegment<byte> contentBytes, out object content)
        {
            content = null;
            try
            {
                using (var stream = new MemoryStream(contentBytes.Array, contentBytes.Offset, contentBytes.Count))
                {
                    using (var reader = new StreamReader(stream, Constants.Utf8NoBOMEncoding))
                    {
                        content = serializer.Deserialize(reader, typeof(object));
                        return content != null;
                    }
                }
            }
            catch
            {
                return false;
            }        
        }

        /// <inheritdoc/>
        public Type Type => typeof(object);
    }
}