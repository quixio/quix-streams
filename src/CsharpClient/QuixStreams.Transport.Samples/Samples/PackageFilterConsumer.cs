using System;
using System.Threading.Tasks;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Samples.Samples
{
    public delegate bool PackageFilter(Package package);

    public class PackageFilterConsumer : IConsumer
    {
        private readonly PackageFilter filter;

        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        /// Initializes new instance of <see cref="PackageFilterConsumer" />
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="filter">Filter to use. When filter returns true, package is kept</param>
        public PackageFilterConsumer(IConsumer consumer, PackageFilter filter)
        {
            this.filter = filter;
            consumer.OnNewPackage = FilterNewPackage;
        }

        private Task FilterNewPackage(Package package)
        {
            if (this.OnNewPackage == null || !this.filter(package)) return Task.CompletedTask;
            return this.OnNewPackage(package);
        }
    }
}