using System;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Transport.Codec;
using QuixStreams.Transport.Fw.Exceptions;
using QuixStreams.Transport.Fw.Helpers;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Registry;

namespace QuixStreams.Transport.Fw
{
    /// <summary>
    /// Modifier, which deserializes the package into the model described in the package.
    /// </summary>
    public class DeserializingModifier : IConsumer, IProducer
    {
        /// <summary>
        /// The callback that is used when deserialized package is available
        /// </summary>
        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        /// Send a package, which the modifier attemptes to deserialize. Deserialization results is raised via <see cref="OnNewPackage"/>
        /// </summary>
        /// <param name="package">The package to deserialize</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting process</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        /// <exception cref="SerializationException">When deserialization fails due to unknown codec or invalid data for codec</exception>
        public Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled(cancellationToken);
            }
            if (!package.TryConvertTo<byte[]>(out var bytePackage) || this.OnNewPackage == null)
            {
                return Task.CompletedTask;
            }
            var packageBytes = bytePackage.Value;

            var transportMessageValue = TransportPackageValueCodec.Deserialize(packageBytes);
            var valueCodec = this.GetCodec(transportMessageValue);
            var lazyVal = this.DeserializeToObject(valueCodec, transportMessageValue);

            var meta = transportMessageValue.MetaData;
            if (bytePackage.MetaData.Count > 0)
            {
                meta = new MetaData(bytePackage.MetaData, meta);
            }

            var newPackage = new Package(valueCodec.Type, lazyVal, meta, bytePackage.TransportContext);
            return this.OnNewPackage(newPackage);
        }

        /// <summary>
        /// Retrieves the codec from the provided package value
        /// </summary>
        /// <param name="transportPackageValue"></param>
        /// <returns>The codec from the package value</returns>
        private ICodec GetCodec(TransportPackageValue transportPackageValue)
        {
            // Is there a specific codec for it?
            var codec = CodecRegistry.RetrieveCodec(transportPackageValue.CodecBundle.ModelKey,
                transportPackageValue.CodecBundle.CodecId);

            if (codec == null)
            {
                throw new MissingCodecException($"Failed to deserialize '{transportPackageValue.CodecBundle.ModelKey}' because there is no codec registered for it.");
            }

            return codec;
        }

        /// <summary>
        /// Deserializes the object into the codec's type
        /// </summary>
        /// <param name="codec">The codec to use</param>
        /// <param name="transportPackageValue">The package value to deserialize</param>
        /// <returns>The deserialized object</returns>
        private object DeserializeToObject(ICodec codec, TransportPackageValue transportPackageValue)
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