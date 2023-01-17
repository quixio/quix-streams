using System;
using System.Threading.Tasks;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport.Samples.Samples
{
    public delegate bool PackageFilter(Package package);

    public class PackageFilterOutput : IOutput
    {
        private readonly PackageFilter filter;

        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        ///     Initializes new instance of <see cref="PackageFilterOutput" />
        /// </summary>
        /// <param name="output"></param>
        /// <param name="filter">Filter to use. When filter returns true, package is kept</param>
        public PackageFilterOutput(IOutput output, PackageFilter filter)
        {
            this.filter = filter;
            output.OnNewPackage = FilterNewPackage;
        }

        private Task FilterNewPackage(Package package)
        {
            if (this.OnNewPackage == null || !this.filter(package)) return Task.CompletedTask;
            return this.OnNewPackage(package);
        }
    }
}