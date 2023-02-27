using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Transport.Fw
{
    /// <summary>
    /// Modifier, which encapsulates an <see cref="IByteSplitter"/> to split packages
    /// </summary>
    public class ByteSplittingModifier : IProducer, IConsumer
    {
        private static int VerboseWarnAboveSegmentCount = 3;
        private static int MovingWarnAboveSize = 0;
        private static ILogger Logger = Logging.CreateLogger(typeof(ByteSplittingModifier));
        private readonly IByteSplitter splitter;

        /// <summary>
        /// Initializes a new instance of <see cref="ByteSplittingModifier"/>
        /// </summary>
        /// <param name="splitter">The splitter to use</param>
        public ByteSplittingModifier(IByteSplitter splitter)
        {
            this.splitter = splitter;
        }

        /// <summary>
        /// The callback that is used when the split package is available
        /// </summary>
        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        /// Publish a package, which the modifier splits if necessary. Split results are raised via <see cref="OnNewPackage"/>
        /// </summary>
        /// <param name="package">The package to split</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting process</param>
        /// <returns>An awaitable <see cref="Task"/>, which resolves when all tasks returned by <see cref="OnNewPackage"/> resolve</returns>
        public Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
            if (this.OnNewPackage == null || !package.TryConvertTo<byte[]>(out var bytePackage)) return Task.CompletedTask;

            var tasks = new List<Task>();
            // this funky logic is done so I know when I'm publishing the last one without enumerating the entire thing into a list first
            // So basically I always send the last one I enumerated, not the current one
            byte[] lastSegment = null;
            Package<byte[]> segmentPackage;
            var value = bytePackage.Value.Value;
            var segmentCount = 0;
            foreach (var segment in this.splitter.Split(value))
            {
                segmentCount++;
                
                if (lastSegment != null)
                {
                    var segmentToReturn = lastSegment; // because lazy gets evaluated later, we need to save reference for this iteration's value
                    segmentPackage = new Package<byte[]>(new Lazy<byte[]>(() => segmentToReturn), null, package.TransportContext);
                    tasks.Add(this.OnNewPackage(segmentPackage));
                }

                lastSegment = segment;
            }
            
            
            if (segmentCount > VerboseWarnAboveSegmentCount)
            {
                
                if (value.Length > MovingWarnAboveSize)
                {
                    // not thread safe, but better than having too many warnings or some performance implication 
                    MovingWarnAboveSize = Math.Max(value.Length, MovingWarnAboveSize) * 2;
                    Logger.LogWarning("One or more of your messages exceed the optimal size. Consider publishing smaller for better consumer experience. Your message was over {0}KB", Math.Round((double)value.Length/1000, 1));
                }
                else
                {
                    Logger.LogTrace("One or more of your messages exceed the optimal size. Consider publishing smaller for better consumer experience. Your message was over {0}KB", Math.Round((double)value.Length/1000, 1));
                }
            }

            if (lastSegment == null) return Task.CompletedTask; // this is probably never a case, but better safe

            segmentPackage = new Package<byte[]>(new Lazy<byte[]>(() => lastSegment), package.MetaData, package.TransportContext);
            tasks.Add(this.OnNewPackage(segmentPackage));

            var taskSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (tasks.Count == 1) return tasks[0];

            Task.WhenAll(tasks).ContinueWith((t) =>
            {
                var isError = tasks.Any(x => x.IsFaulted);
                if (isError)
                {
                    taskSource.SetException(tasks.Where(x => x.IsCompleted).Select(x => x.Exception));
                    return;
                }
                var isCancelled = tasks.Any(x => x.IsCanceled);
                if (isCancelled)
                {
                    taskSource.SetCanceled();
                    return;
                }
                taskSource.SetResult(null);
            });

            return taskSource.Task;
        }
    }
}