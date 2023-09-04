using System;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue
{
    /// <summary>
    /// LEGACY Serialization for the value of Transport Package when the data used to be encoded inside the message's value
    /// Describes how to de/serialize the <see cref="Package.Value"/>
    /// </summary>
    internal sealed class TransportPackageValue
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TransportPackageValue"/>
        /// </summary>
        /// <param name="packageValue">The value to de/serialize </param>
        /// <param name="codecBundle">The codec details to use for de/serialization</param>
        public TransportPackageValue(ArraySegment<byte> packageValue, CodecBundle codecBundle)
        {
            this.Value = packageValue;
            this.CodecBundle = codecBundle;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportPackageValue"/>
        /// </summary>
        /// <param name="packageValue">The value to de/serialize </param>
        /// <param name="codecBundle">The codec details to use for de/serialization</param>
        public TransportPackageValue(byte[] packageValue, CodecBundle codecBundle) :
            this(new ArraySegment<byte>(packageValue), codecBundle) {}

        /// <summary>
        /// The value to de/serialize
        /// </summary>
        public ArraySegment<byte> Value { get; }

        /// <summary>
        /// The codec used to ser/des the value
        /// </summary>
        public CodecBundle CodecBundle { get; }
    }
}