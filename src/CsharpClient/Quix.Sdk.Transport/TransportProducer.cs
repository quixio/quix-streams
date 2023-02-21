using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport
{
    /// <summary>
    /// A prebuilt pipeline, which serializes and optionally splits the provided packages then passes into the specified producer.
    /// </summary>
    public class TransportProducer : IProducer
    {
        private readonly SerializingModifier serializer;

        /// <summary>
        /// Initializes a new instance of <see cref="TransportProducer"/> with the specified <see cref="IProducer"/>
        /// </summary>
        /// <param name="producer">The producer to pass the serialized packages into</param>
        public TransportProducer(IProducer producer) : this(producer, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportProducer"/> with the specified <see cref="IProducer"/> and <see cref="IByteSplitter"/>
        /// </summary>
        /// <param name="producer">The producer to pass the serialized and optionally split packages into</param>
        /// <param name="byteSplitter">The optional byte splitter to use</param>
        public TransportProducer(IProducer producer, IByteSplitter byteSplitter)
        {
            // this -> serializer -?> byteSplitter -> producer
            this.serializer = new SerializingModifier();
            if (byteSplitter != null)
            {
                var splitter = new ByteSplittingModifier(byteSplitter);

                this.serializer.OnNewPackage = e => splitter.Publish(e);
                splitter.OnNewPackage = e => producer.Publish(e);
            }
            else
            {
                this.serializer.OnNewPackage = e => producer.Publish(e);
            }
        }

        /// <summary>
        /// Send a package, which the <see cref="TransportProducer"/> serializes and optionally splits then passes to the provided <see cref="IProducer"/>
        /// </summary>
        /// <param name="package">The package to transport</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting transport</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        public Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
            return this.serializer.Send(package, cancellationToken);
        }
    }
}