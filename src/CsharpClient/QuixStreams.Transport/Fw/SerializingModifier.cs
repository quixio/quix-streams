using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Transport.Codec;
using QuixStreams.Transport.Fw.Codecs;
using QuixStreams.Transport.Fw.Helpers;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Registry;

namespace QuixStreams.Transport.Fw
{
    /// <summary>
    /// Modifier, which serializes the package into bytes
    /// </summary>
    public class SerializingModifier : IConsumer
    {
        /// <summary>
        /// The callback that is used when serialized package is available
        /// </summary>
        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        /// Send a package, which the modifier attemptes to serialize. Serialization result is raised via <see cref="OnNewPackage"/>
        /// Note: <see cref="SerializationException"/> are raised for unsuccessful serialization when package's lazy value is evaluated.
        /// </summary>
        /// <param name="package">The package to serialize</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting process</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        public Task Send(Package package, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
            if (this.OnNewPackage == null) return Task.CompletedTask;
            var modelKey = ModelKeyRegistry.GetModelKey(package.Type);
            if (modelKey == ModelKey.WellKnownModelKeys.Default)
            {
                modelKey = new ModelKey(package.Type);
            }

            var codec = CodecRegistry.RetrieveCodecs(modelKey).FirstOrDefault() ?? throw new SerializationException($"Failed to serialize '{modelKey}' because there is no codec registered for it.");
            var bytePackage = this.SerializePackage(package, codec, new CodecBundle(modelKey, codec.Id));
            return this.OnNewPackage(bytePackage);
        }

        /// <summary>
        /// Serialize Model package into byte package
        /// </summary>
        /// <param name="package">The package to serialize</param>
        /// <param name="codec">The codec to use to serialize the package</param>
        /// <param name="valueCodecBundle">The model details to put inside the package value</param>
        /// <returns>The package serialized lazily</returns>
        private Package<byte[]> SerializePackage(Package package, ICodec codec, CodecBundle valueCodecBundle)
        {
            var value = this.GetSerializedValue(package, codec);
            var transportPackageValue = new TransportPackageValue(value, valueCodecBundle, package.MetaData);
            var serializedTransportPackageValue = TransportPackageValueCodec.Serialize(transportPackageValue);
            
            return new Package<byte[]>(serializedTransportPackageValue, null, package.TransportContext);
        }

        /// <summary>
        /// Serialize the package
        /// </summary>
        /// <param name="package"></param>
        /// <param name="codec"></param>
        /// <returns></returns>
        private byte[] GetSerializedValue(Package package, ICodec codec)
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