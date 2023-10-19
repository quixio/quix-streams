using QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue;

namespace QuixStreams.Kafka.Transport.SerDes
{

    public enum PackageSerializationMode
    {
        /// <summary>
        /// This is our legacy serialization mode, which send the ser/des info within the kafka message's value
        /// </summary>
        LegacyValue,

        /// <summary>
        /// This is our new serialization mode, which send the ser/des info within the kafka message's header
        /// </summary>
        Header
    }

    /// <summary>
    /// The serialization settings to use
    /// </summary>
    public static class PackageSerializationSettings
    {
        /// <summary>
        /// The mode package serialization should be done when publishing data.
        /// For reading, all known protocols are supported by default.
        /// </summary>
        public static PackageSerializationMode Mode { get; set; } = PackageSerializationMode.LegacyValue;


        /// <summary>
        /// The codec type to use for serializing the <see cref="TransportPackage"/>
        /// This is only used when <see cref="Mode"/> is <see cref="PackageSerializationMode.LegacyValue"/>
        /// </summary>
        public static TransportPackageValueCodecType LegacyValueCodecType = TransportPackageValueCodecType.Json;

        /// <summary>
        /// Whether message split is enabled.
        /// Enabled by default
        /// </summary>
        public static bool EnableMessageSplit { get; set; } = true;
    }
}