using System.Diagnostics;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs
{
    /// <summary>
    /// Codec bundle is a composite model with all details necessary for looking up a codec implementation
    /// </summary>
    [DebuggerDisplay("Codec: {CodecId}, Model: {ModelKey}")]
    public sealed class CodecBundle
    {
        /// <summary>
        /// Initializes a new instance of <see cref="CodecBundle" />
        /// </summary>
        /// <param name="modelKey">The key to identify the model</param>
        /// <param name="codecId">The id of the codec used for model serialization</param>
        public CodecBundle(ModelKey modelKey, CodecId codecId)
        {
            this.ModelKey = modelKey;
            this.CodecId = codecId;
        }

        /// <summary>
        /// Gets the key to identify the Data
        /// </summary>
        public ModelKey ModelKey { get; }

        /// <summary>
        /// Gets the codec id to used to serialize the model
        /// </summary>
        public CodecId CodecId { get; }

        /// <summary>
        /// Well known codec bundles
        /// </summary>
        public static class WellKnownCodecBundles
        {
            /// <summary>
            /// String codec bundle
            /// </summary>
            public static readonly CodecBundle String =
                new CodecBundle(ModelKey.WellKnownModelKeys.String, CodecId.WellKnownCodecIds.String);

            /// <summary>
            /// Default code bundle
            /// </summary>
            public static readonly CodecBundle Default =
                new CodecBundle(ModelKey.WellKnownModelKeys.Default, CodecId.WellKnownCodecIds.None);
        }
    }
}