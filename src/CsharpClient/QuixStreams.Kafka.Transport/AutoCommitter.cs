using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace QuixStreams.Kafka.Transport
{
    /// <summary>
    /// Automatic commit handler for packages that are deemed handled
    /// </summary>
    internal sealed class AutoCommitter 
    {
        private readonly Action<ICollection<TopicPartitionOffset>, Action> wrappingCommitCallback;
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<AutoCommitter>();
        private readonly Func<TransportPackage, CancellationToken, Task> onPublish = null;
        private Action closeAction = null;
        private bool closed = false;
        private readonly EventHandler<RevokingEventArgs> revokingHandler = null;
        private readonly EventHandler<RevokedEventArgs> revokedHandler = null;
        
        /// <summary>
        /// this is used to avoid two concurrent commits at the same time. The commits could possibly be a problem
        /// if they happened out of order
        /// </summary>
        private readonly object commitCheckLock = new object();

        private List<TopicPartitionOffset> committableOffsets = new List<TopicPartitionOffset>();
        private bool committing = false;

        /// <summary>
        /// Initializes a new instance of <see cref="AutoCommitter"/>
        /// </summary>
        /// <param name="commitOptions">Auto commit options</param>
        /// <param name="commitCallback">The callback to invoke when conditions occur for a commit</param>
        public AutoCommitter(CommitOptions commitOptions, Action<ICollection<TopicPartitionOffset>> commitCallback)
        {
            if (commitOptions == null) throw new ArgumentNullException(nameof(commitOptions));
            this.wrappingCommitCallback = (offsets, finallyAction) =>
            {
                try
                {
                    committing = true;
                    commitCallback(offsets);
                }
                finally
                {
                    committing = false;
                    finallyAction();
                }
            };

            // copy the settings, not interested in changes.
            var commitEvery = commitOptions.CommitEvery;
            var autoCommit = commitOptions.AutoCommitEnabled;
            var commitInterval = commitOptions.CommitInterval;
            closeAction = () => { };
            
            if (!autoCommit)
            {
                // In case there is no auto committing then all we have to do is pass the message up in the chain
                onPublish = (package, cancellationToken) => this.OnPackageAvailable?.Invoke(package) ?? Task.CompletedTask;
                return;
            }

            if (commitEvery == 1)
            {
                // if we're committing every single message, then any kind of timer based commit is irrelevant
                onPublish = async (package, cancellationToken) =>
                {
                    await (this.OnPackageAvailable?.Invoke(package) ?? Task.CompletedTask);
                    if (package == null) return;
                    if (this.closed) return;
                    logger.LogTrace("Committing offsets due to reaching limit {0}", commitEvery);
                    this.wrappingCommitCallback(new[] { package.KafkaMessage.TopicPartitionOffset }, () => { });
                    logger.LogTrace("Committed offsets due to reaching limit {0}", commitEvery);
                };
                return;
            }

            if (commitInterval > 0)
            {
                var timerEnabled = false; // we use this timer because it is not guaranteed that timer disable actually disables it (more freq. on Linux)
                Timer commitTimer = null;
                void OnElapsed(object state)
                {
                    if (!timerEnabled) return;
                    commitTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
                    lock (commitCheckLock)
                    {
                        if (committableOffsets.Count > 0)
                        {
                            logger.LogTrace("Committing {0} offsets due to timer expiring", committableOffsets.Count);
                            try
                            {
                                this.wrappingCommitCallback(committableOffsets, () => ClearAllOffsets());
                                logger.LogTrace("Committed {0} offsets due to timer expiring", committableOffsets.Count);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, "Failed to commit offsets due to timer expiring.");
                            }
                        }
                    }

                    if (closed) return;
                    timerEnabled = true;
                    commitTimer.Change(commitInterval.Value, Timeout.Infinite); // Reset / Enable flush timer
                }
                commitTimer = new Timer(OnElapsed, null, Timeout.Infinite, Timeout.Infinite); // Create disabled flush timer
                timerEnabled = true;
                commitTimer.Change(commitInterval.Value, Timeout.Infinite); // Reset / Enable flush timer
                this.closeAction += () =>
                {
                    timerEnabled = false;
                    commitTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
                };
            }
            
            this.closeAction += () =>
            {
                lock (commitCheckLock)
                {
                    if (committableOffsets.Count <= 0) return;
                    logger.LogTrace("Committing {0} offsets due to close", committableOffsets.Count);
                    this.wrappingCommitCallback(committableOffsets, () => ClearAllOffsets());
                    logger.LogTrace("Committed {0} offsets due to close", committableOffsets.Count);
                }
            };
            
            if ((commitEvery ?? 0) <= 0)
            {
                // This is a condition where I do not actually want to commit after every N number of messages.
                // The task here is to simply keep track of every message going through this modifier
                onPublish = async (package, cancellationToken) =>
                {
                    await (this.OnPackageAvailable?.Invoke(package) ?? Task.CompletedTask);
                    if (package == null) return;
                    if (this.closed) return;
                    lock (this.commitCheckLock) committableOffsets.Add(package.KafkaMessage.TopicPartitionOffset);
                };
            }
            else
            {
                // This is a condition where I want to commit after every N number of messages.
                onPublish = async (package, cancellationToken) =>
                {
                    await (this.OnPackageAvailable?.Invoke(package) ?? Task.CompletedTask);
                    if (package == null) return;
                    if (this.closed) return;
                    lock (this.commitCheckLock)
                    {
                        committableOffsets.Add(package.KafkaMessage.TopicPartitionOffset);
                        if (committableOffsets.Count == commitEvery)
                        {

                            if (committableOffsets.Count < commitEvery)
                                return; // in case the condition changed after acquiring the lock

                            logger.LogTrace("Committing offsets due to reaching limit {0}", commitEvery);
                            wrappingCommitCallback(committableOffsets, () => ClearAllOffsets());
                            logger.LogTrace("Committed offsets due to reaching limit {0}", commitEvery);
                        }
                    }
                };
            }
        }

        private int ClearCommittedOffsets(ICollection<TopicPartitionOffset> offsetToClear)
        {
            lock (this.commitCheckLock)
            {
                var remove = this.FilterAffectedOffsets(this.committableOffsets, offsetToClear);
                return this.committableOffsets.RemoveAll(y => remove.Contains(y));
            }
        }
        
        private int ClearAllOffsets()
        {
            lock (this.commitCheckLock)
            {
                var count = this.committableOffsets.Count;
                this.committableOffsets = new List<TopicPartitionOffset>();
                return count;
            }
        }
        

        public Task Publish(TransportPackage transportPackage, CancellationToken cancellationToken = default)
        {
            return onPublish(transportPackage, cancellationToken);
        }

        public Func<TransportPackage, Task> OnPackageAvailable { get; set; }
        

        /// <summary>
        /// Close commit modifier
        /// </summary>
        public void Close()
        {
            if (closed) return;
            closed = true;
            this.closeAction.Invoke();
        }

        public void HandleRevoked(RevokedEventArgs args)
        {
            lock (commitCheckLock)
            {
                var offsets = committableOffsets.ToArray();

                if (offsets.Length == 0)
                {
                    return;
                }

                var toClearFilter = FilterAffectedOffsets(offsets, args.Revoked);

                var removed = ClearCommittedOffsets(toClearFilter);

                if (removed > 0)
                {
                    logger.LogDebug("Discarding {0} offsets due to already finished revocation.", offsets.Length);
                }
            }
        }

        public void HandleRevoking(RevokingEventArgs args)
        {
            lock (commitCheckLock)
            {
                var offsets = committableOffsets.ToArray();

                if (offsets.Length == 0)
                {
                    return;
                }

                var offsetsToCommit = FilterAffectedOffsets(offsets, args.Revoking);

                if (offsetsToCommit.Count <= 0) return;
                    
                logger.LogDebug("Committing {0} offsets due to revocation, {1} offsets unaffected.", offsetsToCommit.Count, offsets.Length - offsetsToCommit.Count);
                
                try
                {
                    var latest = ClearCommittedOffsets(offsetsToCommit);
                    wrappingCommitCallback(offsetsToCommit, () => { });
                    logger.LogDebug("Committed {0} offsets due to revocation.", offsetsToCommit.Count);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to commit offsets due to revocation.");
                }
            }
        }
        
        private ICollection<TopicPartitionOffset> FilterAffectedOffsets(ICollection<TopicPartitionOffset> offsets, ICollection<TopicPartitionOffset> commitFilter)
        {
            var limitedCommitFilter = commitFilter.GroupBy(y => y.TopicPartition)
                .Select(y => y.OrderByDescending(z => z.Offset.Value)
                    .First()).ToList();

            return offsets.Where(offset =>
            {
                var affectingFilter =
                    limitedCommitFilter.FirstOrDefault(filter => filter.TopicPartition == offset.TopicPartition);
                if (affectingFilter == null) return false;
                return affectingFilter.Offset.Value >= offset.Offset.Value;
            }).ToList();
        }

        public void HandleCommitted(CommittedEventArgs args)
        {
            if (committing) return;
            var removed = ClearCommittedOffsets(args.Committed.Offsets.Select(y => y.TopicPartitionOffset).ToList());
            this.logger.LogTrace("Cleared {0} offsets due to committed.", removed);
        }

        public void HandleCommitting(CommittingEventArgs args)
        {
            if (committing) return;
            var removed = ClearCommittedOffsets(args.Committing);
            this.logger.LogTrace("Cleared {0} offsets due to committing.", removed);
        }
    }

    /// <summary>
    /// Auto commit options
    /// </summary>
    public sealed class CommitOptions
    {
        /// <summary>
        /// Gets or sets whether automatic committing is enabled.
        /// If automatic committing is not enabled, other values are ignored.
        /// </summary>
        public bool AutoCommitEnabled { get; set; } = true;

        /// <summary>
        /// The interval of automatic commit in ms
        /// </summary>
        public int? CommitInterval { get; set; } = 5000;
        
        /// <summary>
        /// The number of messages to automatically commit at
        /// </summary>
        public int? CommitEvery { get; set; } = 5000;
    } 
}