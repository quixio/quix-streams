using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue;

namespace QuixStreams.Kafka.Transport.SerDes
{
    
    /// <summary>
    /// Serializes <see cref="TransportPackage"/> into <see cref="KafkaMessage"/>
    /// </summary>
    public interface IPackageSerializer
    {
        /// <summary>
        /// Serializes a transport package to a kafka message according to configuration
        /// </summary>
        /// <param name="package">The package to serialize</param>
        /// <exception cref="SerializationException">Raised when serialization fails</exception>
        KafkaMessage Serialize(TransportPackage package);
    }

    /// <summary>
    /// Serializes <see cref="TransportPackage"/> into <see cref="KafkaMessage"/>
    /// </summary>
    public class PackageSerializer : IPackageSerializer
    {
        /// <summary>
        /// Serializes a transport package to a kafka message according to configuration
        /// </summary>
        /// <param name="package">The package to serialize</param>
        /// <exception cref="SerializationException">Raised when serialization fails</exception>
        public KafkaMessage Serialize(TransportPackage package)
        {
            var modelKey = ModelKeyRegistry.GetModelKey(package.Type);
            if (modelKey == ModelKey.WellKnownModelKeys.Default)
            {
                modelKey = new ModelKey(package.Type);
            }

            var codec = CodecRegistry.RetrieveCodecs(modelKey).FirstOrDefault() ?? throw new SerializationException($"Failed to serialize '{modelKey}' because there is no codec registered for it.");
            if (PackageSerializationSettings.Mode == PackageSerializationMode.LegacyValue)
            {
                return this.LegacySerialize(package, codec, new CodecBundle(modelKey, codec.Id));
            }
            
            return this.Serialize(package, codec, new CodecBundle(modelKey, codec.Id));
        }
        
        /// <summary>
        /// Serialize Model transportPackage into byte transportPackage according to legacy value mode
        /// </summary>
        /// <param name="package">The transportPackage to serialize</param>
        /// <param name="codec">The codec to use to serialize the transportPackage</param>
        /// <param name="valueCodecBundle">The model details to put inside the transportPackage value</param>
        /// <returns>The transportPackage serialized lazily</returns>
        private KafkaMessage Serialize(TransportPackage package, ICodec codec, CodecBundle valueCodecBundle)
        {
            var value = this.GetSerializedValue(package, codec);
            
            return new KafkaMessage(Constants.Utf8NoBOMEncoding.GetBytes(package.Key), value, new []
            {
                new KafkaHeader(Constants.KafkaMessageHeaderModelKey, Constants.Utf8NoBOMEncoding.GetBytes(valueCodecBundle.ModelKey)),
                new KafkaHeader(Constants.KafkaMessageHeaderCodecId, Constants.Utf8NoBOMEncoding.GetBytes(valueCodecBundle.CodecId)),
            });
        }

        /// <summary>
        /// Serialize Model transportPackage into byte transportPackage according to legacy value mode
        /// </summary>
        /// <param name="package">The transportPackage to serialize</param>
        /// <param name="codec">The codec to use to serialize the transportPackage</param>
        /// <param name="valueCodecBundle">The model details to put inside the transportPackage value</param>
        /// <returns>The transportPackage serialized lazily</returns>
        private KafkaMessage LegacySerialize(TransportPackage package, ICodec codec, CodecBundle valueCodecBundle)
        {
            var value = this.GetSerializedValue(package, codec);
            var transportPackageValue = new TransportPackageValue(value, valueCodecBundle);
            var serializedTransportPackageValue = TransportPackageValueCodec.Serialize(transportPackageValue, PackageSerializationSettings.LegacyValueCodecType);
            
            return new KafkaMessage(Constants.Utf8NoBOMEncoding.GetBytes(package.Key), serializedTransportPackageValue, null);
        }

        /// <summary>
        /// Serialize the transportPackage
        /// </summary>
        /// <param name="package"></param>
        /// <param name="codec"></param>
        /// <returns></returns>
        private byte[] GetSerializedValue(TransportPackage package, ICodec codec)
        {
            if (codec == null)
            {
                throw new SerializationException($"Failed to serialize type '{package.Type}', because no codec is available");
            }

            if (codec.TrySerialize(package.Value, out var serializedValue))
            {
                return serializedValue;
            }

            throw new SerializationException($"Failed to serialize type with provided codec '{codec.Id}'");
        }
    }
}