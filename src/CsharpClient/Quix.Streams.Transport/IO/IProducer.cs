using System.Threading;
using System.Threading.Tasks;

namespace Quix.Streams.Transport.IO
{
    /// <summary>
    /// Interface to mark class able to publish a <see cref="Package"/>
    /// </summary>
    public interface IProducer
    {
        /// <summary>
        /// Publishes a package
        /// </summary>
        /// <param name="package">The package to publish</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting the process</param>
        Task Publish(Package package, CancellationToken cancellationToken = default);
    }
}