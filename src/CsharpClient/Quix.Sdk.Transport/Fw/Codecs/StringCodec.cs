using Quix.Sdk.Transport.Codec;

namespace Quix.Sdk.Transport.Fw.Codecs
{
    /// <summary>
    /// Codec for serializing and deserializing a <see cref="string"/>
    /// </summary>
    public class StringCodec : Codec<string>
    {
        /// <summary>
        /// The <see cref="StringCodec"/> instance to always use to avoid unnecessary duplication
        /// </summary>
        public static readonly StringCodec Instance = new StringCodec();

        private StringCodec()
        {
        }


        /// <inheritdoc />
        public override CodecId Id => CodecId.WellKnownCodecIds.String;

        /// <inheritdoc />
        public override string Deserialize(byte[] contentBytes)
        {
            return Constants.Utf8NoBOMEncoding.GetString(contentBytes);
        }

        /// <inheritdoc />
        public override byte[] Serialize(string obj)
        {
            return Constants.Utf8NoBOMEncoding.GetBytes(obj);
        }
    }
}