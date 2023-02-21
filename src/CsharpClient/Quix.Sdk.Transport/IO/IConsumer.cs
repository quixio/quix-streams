using System;
using System.Threading.Tasks;

namespace Quix.Sdk.Transport.IO
{
    /// <summary>
    /// Interface for receiving <see cref="Package"/>
    /// </summary>
    public interface IConsumer
    {
        /// <summary>
        /// The callback that is used when the <see cref="IConsumer"/> has new package for the listener
        /// </summary>
        Func<Package, Task> OnNewPackage { get; set; }
    }
}