using System;
using Quix.Streams.Transport.Fw.Codecs;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Transport.Fw.Helpers
{
    /// <summary>
    /// Describes how to de/serialize the <see cref="Package.Value"/>
    /// </summary>
    internal sealed class TransportPackageValue
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TransportPackageValue"/>
        /// </summary>
        /// <param name="packageValue">The value to de/serialize </param>
        /// <param name="codecBundle">The codec details to use for de/serialization</param>
        /// <param name="metaData">The metadata that belongs to the value</param>
        public TransportPackageValue(Lazy<byte[]> packageValue, CodecBundle codecBundle, MetaData metaData = null)
        {
            this.Value = packageValue;
            this.CodecBundle = codecBundle;
            this.MetaData = metaData;
        }

        /// <summary>
        /// The value to de/serialize
        /// </summary>
        public Lazy<byte[]> Value { get; }

        /// <summary>
        /// The codec details to use for de/serialization
        /// </summary>
        public MetaData MetaData { get; }

        /// <summary>
        /// The metadata that belongs to the value
        /// </summary>
        public CodecBundle CodecBundle { get; }
    }
}