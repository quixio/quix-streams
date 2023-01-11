using Quix.Sdk.Process.Models;
using Quix.Sdk.Transport.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Transport.Fw;

namespace Quix.Sdk.Process
{
    /// <summary>
    /// The factory detects new streams from the transport layer and creates new <see cref="IStreamProcess"/>es.
    /// It also maintains a list of active stream processes and the components associated to them.
    /// </summary>
    internal abstract class StreamProcessFactory
    {
        private readonly ILogger logger = Logging.CreateLogger<StreamProcessFactory>();
        private readonly object openCloseLock = new object();
        private bool isOpen;
        private IOutput transportOutput;
        private Func<string, IStreamProcess> streamProcessFactoryHandler;
        private readonly IStreamContextCache contextCache;
        private Action onClose = () => { };
        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        
        
        public event Action<IStreamProcess[]> OnStreamsRevoked;

        private int maxRetryDuration = 60000;

        /// <summary>
        /// The maximum length in ms between each retries when the factory throws exception. Defaults to 60000
        /// </summary>
        public int MaxRetryDuration
        {
            get { return maxRetryDuration; }
            set
            {
                if (value < 50)
                {
                    throw new ArgumentOutOfRangeException(nameof(MaxRetryDuration), "Must be minimum 50.");
                }

                maxRetryDuration = value;
            }
        }

        private int retryIncrease = 5000;

        /// <summary>
        /// The time to increase in ms by between each retries when the factory throws exception. Defaults to 5000
        /// </summary>
        public int RetryIncrease
        {
            get { return retryIncrease; }
            set
            {
                if (value < 50)
                {
                    throw new ArgumentOutOfRangeException(nameof(RetryIncrease), "Must be minimum 50.");
                }

                retryIncrease = value;
            }
        }
        
        /// <summary>
        /// The maximum number of retries. Defaults to -1 (infinite).
        /// </summary>
        public int MaxRetries { get; set; } = -1;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamProcessFactory"/>
        /// </summary>
        /// <param name="transportOutput">Transport layer to read from</param>
        /// <param name="streamProcessFactoryHandler">Handler factory to execute for each Stream detected in the incoming messages in order to create a new <see cref="StreamProcess"/> for each stream. 
        /// The handler function receives a StreamId and has to return a <see cref="StreamProcess"/>.</param>
        /// <param name="contextCache">The cache to store created stream contexts</param>
        public StreamProcessFactory(Transport.IO.IOutput transportOutput, Func<string, IStreamProcess> streamProcessFactoryHandler, IStreamContextCache contextCache)
        {
            this.transportOutput = transportOutput ?? throw new ArgumentNullException(nameof(transportOutput));
            this.streamProcessFactoryHandler = streamProcessFactoryHandler ?? throw new ArgumentNullException(nameof(streamProcessFactoryHandler));
            this.contextCache = contextCache ?? throw new ArgumentNullException(nameof(contextCache));
        }

        /// <summary>
        /// Starts reading subscription from Transport layer
        /// </summary>
        public void Open()
        {
            if (this.isOpen) return;
            lock (this.openCloseLock)
            {
                if (this.isOpen) return;
                this.cancellationTokenSource = new CancellationTokenSource();
                var output = this.transportOutput;
                if (output == null) return;
                this.onClose = () =>
                {
                    cancellationTokenSource.Cancel();
                };

                output.OnNewPackage += this.NewTransportPackageHandler;
                this.onClose += () => { output.OnNewPackage -= this.NewTransportPackageHandler; };

                if (output is IRevocationPublisher revocationPublisher)
                {
                    revocationPublisher.OnRevoked += this.OutputRevokedHandler;
                    this.onClose += () => { revocationPublisher.OnRevoked -= this.OutputRevokedHandler; };
                }
                if (output is ICanCommit canCommit)
                {
                    canCommit.OnCommitted += this.OutputCommittedHandler;
                    this.onClose += () => { canCommit.OnCommitted -= this.OutputCommittedHandler; };
                }
                this.isOpen = true;
            }
        }

        private void OutputCommittedHandler(object sender, OnCommittedEventArgs args)
        {
            var committer = (ICanCommit) sender;
            lock (this.contextCache.Sync)
            {
                var streamContexts = this.contextCache.GetAll();
                var transportContext = streamContexts.Select(y => y.Value.LastUncommittedTransportContext).Where(x => x != null).ToList();
                var affectedContexts = committer.FilterCommittedContexts(args.State, transportContext).ToList();
                if (affectedContexts.Count == 0) return;

                var affectedStreamContexts = affectedContexts.Join(streamContexts,
                    tContext => tContext,
                    sContext => sContext.Value.LastUncommittedTransportContext,
                    (tContext, sContext) => sContext).ToList();
                
                Debug.Assert(affectedStreamContexts.Count == affectedContexts.Count);
                
                foreach (var affectedStreamContext in affectedStreamContexts)
                {
                    affectedStreamContext.Value.LastUncommittedTransportContext = null;
                }
            }
        }

        protected virtual void OutputRevokedHandler(object sender, OnRevokedEventArgs args)
        {
            var publisher = (IRevocationPublisher) sender;
            lock (this.contextCache.Sync)
            {
                var streamContexts = this.contextCache.GetAll();
                var transportContext = streamContexts.Select(y => y.Value.LastTransportContext).Where(x => x != null).ToList();
                var affectedContexts = publisher.FilterRevokedContexts(args.State, transportContext).ToList();
                if (affectedContexts.Count == 0) return;

                var affectedStreamContexts = affectedContexts.Join(streamContexts,
                    tContext => tContext,
                    sContext => sContext.Value.LastTransportContext,
                    (tContext, sContext) => sContext).ToList();
                
                OnStreamsRevoked?.Invoke(affectedStreamContexts.Select(y=> y.Value.StreamProcess).ToArray());
                
                foreach (var affectedContext in affectedStreamContexts)
                {
                    try
                    {
                        affectedContext.Value.StreamProcess.Close();
                    }
                    catch (Exception ex)
                    {
                        logger.LogTrace(ex, "Exception while closing stream process after revocation.");
                        // Ignore fails, they may have been closed in a callback of OnStreamsRevoked
                    }
                }
            }
        }

        /// <summary>
        /// Attempts to retrieve the streamId from the package
        /// </summary>
        /// <param name="package">The package to retrieve the streamId from</param>
        /// <param name="streamId">The streamId retrieved</param>
        /// <returns>Whether retrieval was successful</returns>
        protected abstract bool TryGetStreamId(TransportContext package, out string streamId);

        private Task NewTransportPackageHandler(Package package)
        {
            if (package == null)
            {
                this.logger.LogWarning("StreamProcessFactory: Null package. Malformed package?");
                return Task.CompletedTask;
            }
            
            if (package.TransportContext == null)
            {
                this.logger.LogWarning("StreamProcessFactory: failed to get stream id from message due to lack of transport context. Malformed package?");
                return Task.CompletedTask;;
            }
            if (!this.TryGetStreamId(package.TransportContext, out var streamId))
            {
                this.logger.LogWarning("StreamProcessFactory: failed to get stream id from message. Malformed package?");
                return Task.CompletedTask;;
            }

            StreamContext streamContext;
            lock (this.contextCache.Sync) // Locking due to updating a streamContext
            {
                if (!this.contextCache.TryGet(streamId, out streamContext))
                {
                    streamContext = new StreamContext(streamId);
                    if (!this.contextCache.TryAdd(streamContext))
                    {
                        this.logger.LogError("StreamProcessFactory: failed to cache stream context. {0}", streamContext.StreamId);
                        return Task.CompletedTask;;
                    }

                    this.logger.LogTrace("StreamProcessFactory: package is for a new stream");
                    // Create the new Stream Process with the Stream Factory handler
                    // Stream Process class stands for a specific Stream with its own processes and state (if it exists)
                    var retryCount = 0;
                    do
                    {
                        try
                        {
                            streamContext.StreamProcess = this.streamProcessFactoryHandler.Invoke(streamId);
                            break; // success, no retry
                        }
                        catch (Exception ex)
                        {                           
                            retryCount++;
                            if (retryCount >= this.MaxRetries && this.MaxRetries != -1)
                            {
                                this.contextCache.Remove(streamId);
                                throw new Exception($"Exception while creating a new stream process for stream {streamId}. Failed {retryCount} times. Reached maximum retry count.", ex);
                            }
                            var waitFor = Math.Min(retryCount * this.RetryIncrease, this.MaxRetryDuration); 
                            this.logger.LogError(ex, "Exception while creating a new stream process for stream {0}. Failed {1} times. Waiting {2}ms then retrying again.", streamId, retryCount, waitFor);
                            try
                            {
                                this.cancellationTokenSource.Token.WaitHandle.WaitOne(
                                    TimeSpan.FromMilliseconds(waitFor)); // await is not possible within lock, but we want to block. Not doing so would run risk of data loss
                            }
                            catch
                            {
                                if (this.cancellationTokenSource.IsCancellationRequested) return Task.FromCanceled(this.cancellationTokenSource.Token);
                            }
                        }
                    } while (true); // the inner breaks/throws will deal with this

                    // Saving Transport metadata for discretionary usings by Stream Components
                    streamContext.StreamProcess.SourceMetadata = new Dictionary<string, string>(package.TransportContext.ToDictionary(kv => kv.Key, kv => kv.Value.ToString()));


                    // Close the stream process if we received an StreamEnd message
                    this.logger.LogTrace("StreamProcessFactory: subscribing to StreamEnd for the new stream. {0}", streamContext.StreamId);
                    streamContext.StreamProcess.OnClosing += () =>
                    {
                        this.contextCache.Remove(streamContext.StreamId);
                        this.logger.LogTrace("StreamProcessFactory: Removed stream from dictionary of streams {0} due to stream closing", streamContext.StreamId);
                    };
                    streamContext.StreamProcess.Subscribe<StreamEnd>(OnStreamFinished);
                }

                streamContext.LastUncommittedTransportContext = package.TransportContext;
                streamContext.LastTransportContext = package.TransportContext;
            }

            // Convert Transport Package to Stream Package
            var streamPackage = new StreamPackage(package);

            // Send the Stream Package to the stream
            return streamContext.StreamProcess.Send(streamPackage);
        }

        /// <summary>
        /// Close reading subscription from Transport layer and close all the Stream processes managed by the factory
        /// </summary>
        public void Close()
        {
            if (!this.isOpen) return;
            lock (this.openCloseLock)
            {
                if (!this.isOpen) return;
                this.isOpen = false;

                this.transportOutput = null;

                lock (this.contextCache.Sync)
                {
                    // Close the streams
                    foreach (var keyValuePair in this.contextCache.GetAll())
                    {
                        keyValuePair.Value.StreamProcess.Close();
                    }
                }

                onClose();
            }
        }

        // Close the stream process if we received an StreamEnd message
        private void OnStreamFinished(IStreamProcess stream, StreamEnd obj)
        {
            this.logger.LogTrace("StreamProcessFactory: OnStreamFinished -> closing stream");
            stream.Close();
            this.logger.LogTrace("StreamProcessFactory: OnStreamFinished -> stream closed");
        }

    }
}