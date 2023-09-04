using System;
using System.Linq;
using System.Runtime.Serialization;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue;

namespace QuixStreams.Kafka.Transport.SerDes
{
    /// <summary>
    /// Deserializes <see cref="KafkaMessage"/> into <see cref="TransportPackage"/>
    /// </summary>
    public class PackageDeserializer
    {
        /// <summary>
        /// Deserializes a <see cref="KafkaMessage"/> into <see cref="TransportPackage"/>
        /// </summary>
        /// <param name="message">The transportPackage to deserialize</param>
        /// <returns>The deserialized <see cref="TransportPackage"/></returns>
        /// <exception cref="SerializationException">If codec is missing or fails to deserialize with it</exception>
        public TransportPackage Deserialize(KafkaMessage message)
        {
            if (TryDeserializeUsingHeader(message, out var package)) return package;

            return LegacyDeserialize(message);
        }

        /// <summary>
        /// Attempts to deserialize the message using the headers for serdes info. If not a header based package, returns false
        /// else the value or exception.
        /// </summary>
        /// <param name="message">The message to deserialize</param>
        /// <param name="package">The resulting package</param>
        /// <returns>Whether it is a header based package</returns>
        /// <exception cref="SerializationException">If codec is missing or fails to deserialize with it</exception>
        private bool TryDeserializeUsingHeader(KafkaMessage message, out TransportPackage package)
        {
            package = null;
            if (message.Headers == null) return false;
            var codecIdBytes = message.Headers?.FirstOrDefault(y => 
                    y.Key == Constants.KafkaMessageHeaderCodecId)?.Value;
            if (codecIdBytes == null)
            {
                return false;
            }

            var modelKeyBytes = message.Headers?.FirstOrDefault(y=> 
                    y.Key == Constants.KafkaMessageHeaderModelKey)?.Value;
            if (modelKeyBytes == null)
            {
                return false;
            }

            var codecId = Constants.Utf8NoBOMEncoding.GetString(codecIdBytes);
            var modelKey = Constants.Utf8NoBOMEncoding.GetString(modelKeyBytes);

            var codec = CodecRegistry.RetrieveCodec(modelKey, codecId);

            if (codec == null)
                throw new SerializationException(
                    $"Missing Codec with model key '{modelKey}' and codec id '{codecId}'.");

            if (!codec.TryDeserialize(message.Value, out var valueObject))
            {
                throw new SerializationException($"Failed to deserialize '{modelKey}' with codec '{codecId}'");
            }

            var key = Constants.Utf8NoBOMEncoding.GetString(message.Key);

            package = new TransportPackage(codec.Type, key, valueObject, message);
            return true;
        }
        
        /// <summary>
        /// Attempts to deserialize the message using the VALUE for serdes info. If not a header based package, returns false
        /// else the value or exception.
        /// </summary>
        /// <param name="message">The message to deserialize</param>
        /// <returns>Whether it is a header based package</returns>
        /// <exception cref="SerializationException">If codec is missing or fails to deserialize with it</exception>
        private TransportPackage LegacyDeserialize(KafkaMessage message)
        {
            var transportPackageValue = TransportPackageValueCodec.Deserialize(message.Value);
            var valueCodec = this.LegacyGetCodec(transportPackageValue.CodecBundle);
            var value = this.LegacyDeserializeToObject(valueCodec, transportPackageValue);
            var key = message.Key == null ? string.Empty : Constants.Utf8NoBOMEncoding.GetString(message.Key);


            var transportPackage = new TransportPackage(valueCodec.Type, key, value, message);
            return transportPackage;
        }

        /// <summary>
        /// Retrieves the 
        /// </summary>
        /// <param name="codecBundle"></param>
        /// <returns></returns>
        private ICodec LegacyGetCodec(CodecBundle codecBundle)
        {
            // Is there a specific codec for it?
            var codec = CodecRegistry.RetrieveCodec(codecBundle.ModelKey, codecBundle.CodecId);

            return codec ?? ByteCodec.Instance;
        }

        /// <summary>
        /// Deserializes the object into the codec's type
        /// </summary>
        /// <param name="codec">The codec to use</param>
        /// <param name="transportPackageValue">The transportPackage value to deserialize</param>
        /// <returns>The deserialized object</returns>
        private object LegacyDeserializeToObject(ICodec codec, TransportPackageValue transportPackageValue)
        {
            // Is there a specific codec for it?
            if (!codec.TryDeserialize(transportPackageValue.Value, out var obj))
            {
                throw new SerializationException($"Failed to deserialize '{transportPackageValue.CodecBundle.ModelKey}' with codec '{codec.Id}'");
            }

            return obj;
        }
    }
}