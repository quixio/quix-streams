using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Fw
{
    /// <summary>
    /// Modifier, which encapsulates an <see cref="IByteMerger"/> to merge previously split packages
    /// </summary>
    public class ByteMergingModifier : IProducer, IConsumer, IRevocationSubscriber
    {
        private readonly IByteMerger merger;
        private readonly ILogger logger = Logging.CreateLogger<ByteMergingModifier>();

        private long bufferOrder = 0;
        private long bufferCounter = 0;
        private readonly ConcurrentDictionary<string, Package> pendingPackages = new ConcurrentDictionary<string, Package>(); // Packages that are queued up. If they have package value, there is no more fragment to merge
        private readonly ConcurrentDictionary<string, long> packageOrder = new ConcurrentDictionary<string, long>(); // the order the packages should be raised
        private readonly ConcurrentDictionary<string, TransportContext> firstPackageContext = new ConcurrentDictionary<string, TransportContext>(); // In case of packages that need merging, this package is the one which contains the extra context.
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1,1); // see https://blog.cdemi.io/async-waiting-inside-c-sharp-locks/

        /// <summary>
        /// Initializes a new instance of <see cref="ByteMergingModifier"/>
        /// </summary>
        /// <param name="merger">The merger to use</param>
        public ByteMergingModifier(IByteMerger merger)
        {
            this.merger = merger;
            this.merger.OnMessageSegmentsPurged += (bufferId) =>
            {
                if (this.RemoveFromBuffer(bufferId))
                {
                    RaiseNextPackageIfReady().GetAwaiter().GetResult();
                }
            };
        }

        /// <summary>
        /// The callback that is used when merged package is available
        /// </summary>
        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        /// Publish a package, which the modifier attempts to merge. Merge results are raised via <see cref="OnNewPackage"/>
        /// </summary>
        /// <param name="package">The package to merge</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting process</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
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

            var mergedPackageBytes = this.merger.Merge(bytePackage, out var bufferId);

            if (mergedPackageBytes == null)
            {
                // if we do not have a merged package, then that means that this was not a standalone package, and the 
                // content of this package was not enough to create a merged package.
                if (bufferId == null)
                {
                    // Means that the package is invalid, due to buffering // missing data constraints
                    return Task.CompletedTask;
                } 
                TryAddToBuffer(ref bufferId, null, bytePackage.TransportContext);
                return Task.CompletedTask;
            }

            // By this point the merged package bytes can't be null, meaning that it was either a package that never had
            // any merging to do, or it was a package segment which completed a full package.
            Package packageToRaise = null;
            if (bufferId == null)
            {
                // null buffer id means that this is not a merged package
                // lets use original package completely
                packageToRaise = bytePackage;
            }
            else
            {
                // buffer id means that this is a merged package
                this.firstPackageContext.TryGetValue(bufferId, out var transportContext);
                packageToRaise = new Package<byte[]>(mergedPackageBytes, bytePackage.MetaData, transportContext);
                
            }
            
            // check if empty. We're not worried about threading here, because this method is designed to be invoked via single thread
            // and any external thread will only ever reduce it, not increment. (see OnMessageSegmentsPurged)
            if (this.bufferCounter == 0)
            {
                RemoveFromBuffer(bufferId);
                return this.OnNewPackage(packageToRaise);
            }
            
            // Not empty, check if this is next in line
            if (bufferId == null)
            {
                // can't be next in line. No buffer id tells us it isn't a buffered value. Given there are other values in the buffer already, this can't possibly be the next.
                TryAddToBuffer(ref bufferId, bytePackage, null);
            }
            else
            {
                // Could be next, but we don't know yet. Let's update in the buffer
                this.pendingPackages.TryUpdate(bufferId, packageToRaise, null);
            }

            return RaiseNextPackageIfReady();
        }

        private async Task RaiseNextPackageIfReady()
        {
            // The logic here has to be locked, because it touches multiple objects based on condition of other ones
            await semaphoreSlim.WaitAsync();
            try
            {
                // lets figure out what is the next
                var orderedPairs = packageOrder.OrderBy(y=> y.Value).ToList();
                foreach (var pair in orderedPairs)
                {
                    var nextBufferId = pair.Key;
                    if (!pendingPackages.TryGetValue(nextBufferId, out var nextPackage))
                        continue; // eh ? did it get removed ?
                    if (nextPackage == null)
                    {
                        // the next package in line is not yet ready.
                        return;
                    }

                    await this.OnNewPackage(nextPackage); 
                    RemoveFromBuffer(nextBufferId);
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private bool TryAddToBuffer(ref string bufferId, Package value, TransportContext transportContext)
        {
            long order; 
            if (bufferId == null) 
            {
                order = Interlocked.Increment(ref bufferOrder);
                bufferId = order.ToString(); // not worried about buffer removal here, because only case this should happen if it never was buffered
                if (!pendingPackages.TryAdd(bufferId, value)) return false; // not the end of the world to not reduce bufferOrder even if failing... however failure here is "a probably never"
            }
            else
            {
                if (!pendingPackages.TryAdd(bufferId, value)) return false;
                 order = Interlocked.Increment(ref bufferOrder);
            }
            packageOrder.TryAdd(bufferId, order);
            if (transportContext != null) firstPackageContext.TryAdd(bufferId, transportContext);
            Interlocked.Increment(ref bufferCounter);
            return true;
        }
        
        private bool RemoveFromBuffer(string bufferId)
        {
            if (bufferId == null) return false;
            if (!pendingPackages.TryRemove(bufferId, out _)) return false;
            packageOrder.TryRemove(bufferId, out var order);
            if (!firstPackageContext.TryRemove(bufferId, out _)) return false; // this is not a split package. It is a queued package that is already whole and and isn't buffer
            this.merger.Purge(bufferId);
            var bOrder = Interlocked.Read(ref bufferOrder);
            if (Interlocked.Decrement(ref bufferCounter) == 0)
            {
                Interlocked.CompareExchange(ref bufferOrder, 0, bOrder); // if still the value we thought before resetting buffer counter to 0, then set to 0 also. Overflow would not likely be a problem ever, but better safe
            }

            return true;
        }
        
        /// <inheritdoc/>
        public void Subscribe(IRevocationPublisher revocationPublisher)
        {
            revocationPublisher.OnRevoked += (sender, state) =>
            {
                var merges = this.firstPackageContext.ToArray();
                if (merges.Length == 0) return; // there is nothing to do
                var contexts = merges.Select(y => y.Value).ToList();
                var filtered = revocationPublisher.FilterRevokedContexts(state, contexts).ToList();
                var affectedBufferIds = filtered.Join(merges, context => context, merge => merge.Value, (context, merge) => merge.Key).ToList();
                if (affectedBufferIds.Count == 0) return;
                foreach (var affectedBufferId in affectedBufferIds)
                {
                    this.RemoveFromBuffer(affectedBufferId);
                }
                RaiseNextPackageIfReady().GetAwaiter().GetResult();
            };
        }
    }
}