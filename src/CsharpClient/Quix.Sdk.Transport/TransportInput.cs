using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport
{
    /// <summary>
    /// A prebuilt pipeline, which serializes and optionally splits the provided packages then passes into the specified input.
    /// </summary>
    public class TransportInput : IInput
    {
        private readonly SerializingModifier serializer;

        /// <summary>
        /// Initializes a new instance of <see cref="TransportInput"/> with the specified <see cref="IInput"/>
        /// </summary>
        /// <param name="input">The input to pass the serialized packages into</param>
        public TransportInput(IInput input) : this(input, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportInput"/> with the specified <see cref="IInput"/> and <see cref="IByteSplitter"/>
        /// </summary>
        /// <param name="input">The input to pass the serialized and optionally split packages into</param>
        /// <param name="byteSplitter">The optional byte splitter to use</param>
        public TransportInput(IInput input, IByteSplitter byteSplitter)
        {
            // this -> serializer -?> byteSplitter -> input
            this.serializer = new SerializingModifier();
            if (byteSplitter != null)
            {
                var splitter = new ByteSplittingModifier(byteSplitter);

                this.serializer.OnNewPackage = e => splitter.Send(e);
                splitter.OnNewPackage = e => input.Send(e);
            }
            else
            {
                this.serializer.OnNewPackage = e => input.Send(e);
            }
        }

        /// <summary>
        /// Send a package, which the <see cref="TransportInput"/> serializes and optionally splits then passes to the provided <see cref="IInput"/>
        /// </summary>
        /// <param name="package">The package to transport</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting transport</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        public Task Send(Package package, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
            return this.serializer.Send(package, cancellationToken);
        }
    }
}