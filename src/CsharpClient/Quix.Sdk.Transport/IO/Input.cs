using System.Threading;
using System.Threading.Tasks;

namespace Quix.Sdk.Transport.IO
{
    /// <summary>
    ///     Interface for providing a class a way to take <see cref="Package"/>
    /// </summary>
    public interface IInput
    {
        /// <summary>
        /// Send a package, which the input may process asynchronously
        /// </summary>
        /// <param name="package">The package to send</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting process</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        Task Send(Package package, CancellationToken cancellationToken = default);
    }
}