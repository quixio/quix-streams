using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Transport.Fw
{
    /// <summary>
    /// Commit modifier which enables committing packages in an automatic fashion
    /// </summary>
    public sealed class CommitModifier : IProducer, IConsumer, ICanCommit, ICanCommitSubscriber, IRevocationSubscriber
    {
        private readonly ILogger logger = Logging.CreateLogger<CommitModifier>();
        private readonly Func<Package, CancellationToken, Task> onPublish = null;
        private Action onClose = null;
        private bool closed = false;
        private readonly EventHandler<OnRevokingEventArgs> onRevoking = null;
        private readonly EventHandler<OnRevokedEventArgs> onRevoked = null;
        private ICanCommit committer;

        /// <summary>
        /// Initializes a new instance of <see cref="CommitModifier"/>
        /// </summary>
        /// <param name="commitOptions">Auto commit options</param>
        public CommitModifier(CommitOptions commitOptions)
        {
            if (commitOptions == null) throw new ArgumentNullException(nameof(commitOptions));
            
            // copy the settings, not interested in changes.
            var commitEvery = commitOptions.CommitEvery;
            var autoCommit = commitOptions.AutoCommitEnabled;
            var commitInterval = commitOptions.CommitInterval;
            onClose = () => { };
            
            if (!autoCommit)
            {
                // In case there is no auto committing then all we have to do is pass the message up in the chain
                onPublish = (package, cancellationToken) => this.OnNewPackage?.Invoke(package) ?? Task.CompletedTask;
                return;
            }

            if (commitEvery == 1)
            {
                // if we're committing every single message, then any kind of timer based commit is irrelevant
                onPublish = async (package, cancellationToken) =>
                {
                    await (this.OnNewPackage?.Invoke(package) ?? Task.CompletedTask);
                    if (package == null) return;
                    if (this.closed) return;
                    logger.LogTrace("Committing contexts due to reaching limit {0}", commitEvery);
                    this.Commit(package.TransportContext);
                    logger.LogTrace("Committed contexts due to reaching limit {0}", commitEvery);
                };
                return;
            }
            
            var contextsReadyToCommit = new ConcurrentQueue<TransportContext>();
            var commitCheckLock = new object(); // this is used to avoid two concurrent commits at the same time. The commits could possibly be a problem
                                                // if they happened out of order
            
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
                        var toCommit = contextsReadyToCommit.ToArray();

                        if (toCommit.Length > 0)
                        {
                            logger.LogTrace("Committing {0} contexts due to timer expiring", toCommit.Length);
                            try
                            {
                                this.Commit(toCommit);
                                logger.LogTrace("Committed {0} contexts due to timer expiring", toCommit.Length);
                                AcknowledgeTransportContext(contextsReadyToCommit, toCommit);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, "Failed to commit contexts due to timer expiring.");
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
                this.onClose += () =>
                {
                    timerEnabled = false;
                    commitTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
                };
            }
            
            onRevoking = (sender, args) =>
            {
                lock (commitCheckLock)
                {
                    var contexts = contextsReadyToCommit.ToArray();

                    if (contexts.Length == 0)
                    {
                        return;
                    }

                    var affectedContexts = ((IRevocationPublisher) sender).FilterRevokedContexts(args.State, contexts).ToArray();

                    if (affectedContexts.Length <= 0) return;
                    
                    logger.LogDebug("Committing {0} contexts due to revocation, {1} contexts unaffected.", affectedContexts.Length, contexts.Length - affectedContexts.Length);
                    try
                    {
                        this.Commit(affectedContexts);
                        logger.LogDebug("Committed {0} contexts due to revocation.", affectedContexts.Length);
                            
                        // Theoretically it is possible to have a concurrency issue here. The issue is that the old queue gets enumerated and creates a new queue.
                        // Then it gets assigned to the variable. Between creating the new queue and assigning it to the variable another thread could still add to the
                        // old queue which would no longer get evaluated.
                        contextsReadyToCommit = new ConcurrentQueue<TransportContext>(contextsReadyToCommit.Except(affectedContexts));
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to commit contexts due to revocation.");
                    }
                }
            };
            
            onRevoked = (sender, args) =>
            {
                lock (commitCheckLock)
                {
                    var contexts = contextsReadyToCommit.ToArray();

                    if (contexts.Length == 0)
                    {
                        return;
                    }

                    var affectedContexts = ((IRevocationPublisher) sender).FilterRevokedContexts(args.State, contexts).ToArray();

                    // Theoretically it is possible to have a concurrency issue here. The issue is that the old queue gets enumerated and creates a new queue.
                    // Then it gets assigned to the variable. Between creating the new queue and assigning it to the variable another thread could still add to the
                    // old queue which would no longer get evaluated.
                    contextsReadyToCommit = new ConcurrentQueue<TransportContext>(contextsReadyToCommit.Except(affectedContexts));

                    if (affectedContexts.Length > 0)
                    {
                        logger.LogWarning("Discarding {0} contexts due to already finished revocation.", contexts.Length);
                    }
                }
            };
            
            this.onClose += () =>
            {
                lock (commitCheckLock)
                {
                    var toCommit = contextsReadyToCommit.ToArray();

                    if (toCommit.Length <= 0) return;
                    logger.LogTrace("Committing {0} contexts due to close", toCommit.Length);
                    this.Commit(toCommit);
                    logger.LogTrace("Committed {0} contexts due to close", toCommit.Length);
                    AcknowledgeTransportContext(contextsReadyToCommit, toCommit);
                }
            };
            
            if ((commitEvery ?? 0) <= 0)
            {
                // This is a condition where I do not actually want to commit after every N number of messages.
                // The task here is to simply keep track of every message going through this modifier
                onPublish = async (package, cancellationToken) =>
                {
                    await (this.OnNewPackage?.Invoke(package) ?? Task.CompletedTask);
                    if (package == null) return;
                    if (this.closed) return;
                    contextsReadyToCommit.Enqueue(package.TransportContext);
                };
            }
            else
            {
                // This is a condition where I want to commit after every N number of messages.
                onPublish = async (package, cancellationToken) =>
                {
                    await (this.OnNewPackage?.Invoke(package) ?? Task.CompletedTask);
                    if (package == null) return;
                    if (this.closed) return;
                    contextsReadyToCommit.Enqueue(package.TransportContext);
                    
                    if (contextsReadyToCommit.Count >= commitEvery)
                    {
                        lock (commitCheckLock)
                        {
                            if (contextsReadyToCommit.Count < commitEvery) return; // in case the condition changed after acquiring the lock

                            var toCommit = contextsReadyToCommit.Take(commitEvery.Value).ToArray();

                            logger.LogTrace("Committing contexts due to reaching limit {0}", commitEvery);
                            this.Commit(toCommit);
                            logger.LogTrace("Committed contexts due to reaching limit {0}", commitEvery);
                            AcknowledgeTransportContext(contextsReadyToCommit, toCommit);
                        }
                    }
                };
            }
        }

        private void AcknowledgeTransportContext(ConcurrentQueue<TransportContext> queue, TransportContext[] acknowledge)
        {
            foreach (var transportContext in acknowledge)
            {
                if (!queue.TryPeek(out var queueContext))
                {
                    this.logger.LogWarning("Tried to acknowledge more item than there is in the queue.");
                    return;
                }

                if (queueContext != transportContext)
                {
                    this.logger.LogWarning("The next item in the transport queue is not what is expected. Avoiding acknowledging item.");
                    return;
                }

                queue.TryDequeue(out var _); // at this point we are certain of what is getting dequeued, so no checking.
            }
        }

        /// <inheritdoc/>
        public Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            return onPublish(package, cancellationToken);
        }

        /// <inheritdoc/>
        public Func<Package, Task> OnNewPackage { get; set; }
        
        /// <inheritdoc/>
        public void Commit(TransportContext[] transportContexts)
        {
            this.committer?.Commit(transportContexts);
        }
        
        /// <inheritdoc/>
        public event EventHandler<OnCommittedEventArgs> OnCommitted;

        /// <inheritdoc/>
        public event EventHandler<OnCommittingEventArgs> OnCommitting;

        /// <inheritdoc/>
        public IEnumerable<TransportContext> FilterCommittedContexts(object state, IEnumerable<TransportContext> contextsToFilter)
        {
            return this.committer?.FilterCommittedContexts(state, contextsToFilter) ?? Array.Empty<TransportContext>();
        }

        /// <summary>
        /// Close commit modifier
        /// </summary>
        public void Close()
        {
            if (closed) return;
            closed = true;
            this.onClose.Invoke();
        }

        /// <inheritdoc/>
        public void Subscribe(ICanCommit committer)
        {
            if (this.committer != null)
            {
                throw new InvalidOperationException("Already connected to a committer");
            }
            this.committer = committer;

            void CommittedHandler(object sender, OnCommittedEventArgs args)
            {
                this.OnCommitted?.Invoke(sender, args);
            }
            
            void CommittingHandler(object sender, OnCommittingEventArgs args)
            {
                this.OnCommitting?.Invoke(sender, args);
            }

            committer.OnCommitted += CommittedHandler;
            committer.OnCommitting += CommittingHandler;
            this.onClose += () =>
            {
                committer.OnCommitted -= CommittedHandler;
                committer.OnCommitting -= CommittingHandler;
            };
        }

        /// <inheritdoc/>
        public void Subscribe(IRevocationPublisher revocationPublisher)
        {
            if (this.onRevoked != null)
            {
                revocationPublisher.OnRevoked += this.onRevoked;
                this.onClose += () => revocationPublisher.OnRevoked -= this.onRevoked;
            }

            if (this.onRevoking != null)
            {
                revocationPublisher.OnRevoking += this.onRevoking;
                this.onClose += () => revocationPublisher.OnRevoking -= this.onRevoking;
            }
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