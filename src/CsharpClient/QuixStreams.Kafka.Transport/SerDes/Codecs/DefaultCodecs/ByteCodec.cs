using System;
using System.Linq;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs
{
    /// <summary>
    /// Codec for serializing and deserializing a <see cref="string"/>
    /// </summary>
    public class ByteCodec : Codec<byte[]>
    {
        /// <summary>
        /// The <see cref="StringCodec"/> instance to always use to avoid unnecessary duplication
        /// </summary>
        public static readonly ByteCodec Instance = new ByteCodec();

        private ByteCodec()
        {
        }


        /// <inheritdoc />
        public override CodecId Id => CodecId.WellKnownCodecIds.Byte;

        /// <inheritdoc />
        public override byte[] Deserialize(byte[] contentBytes)
        {
            return contentBytes;
        }

        /// <inheritdoc />
        public override byte[] Deserialize(ArraySegment<byte> contentBytes)
        {
            return contentBytes.ToArray();
        }

        /// <inheritdoc />
        public override byte[] Serialize(byte[] obj)
        {
            return obj;
        }
    }
}