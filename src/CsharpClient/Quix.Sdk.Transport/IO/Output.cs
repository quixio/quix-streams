using System;
using System.Threading.Tasks;

namespace Quix.Sdk.Transport.IO
{
    /// <summary>
    ///     Interface for providing a class a way to push <see cref="Package"/> to listener
    /// </summary>
    public interface IOutput
    {
        /// <summary>
        /// The callback that is used when the <see cref="IOutput"/> has new package for the listener
        /// </summary>
        Func<Package, Task> OnNewPackage { get; set; }
    }
}