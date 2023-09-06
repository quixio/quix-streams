namespace QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue
{
    /// <summary>
    /// LEGACY Serialization for Transport Package Value when the data used to be encoded inside the message's value
    /// Codec type for serializing a <see cref="TransportPackageValue"/>
    /// </summary>
    public enum TransportPackageValueCodecType
    {
        /// <summary>
        /// Binary codec
        /// </summary>
        Binary,
        
        /// <summary>
        /// Json codec
        /// </summary>
        Json
    }
}