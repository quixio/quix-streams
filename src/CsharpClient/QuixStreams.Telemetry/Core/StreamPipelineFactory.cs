using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams.Kafka;
using QuixStreams;
using QuixStreams.Kafka.Transport;
using QuixStreams.Telemetry.Models;


namespace QuixStreams.Telemetry
{
    /// <summary>
    /// The factory detects new streams from the transport layer and creates new <see cref="IStreamPipeline"/>es.
    /// It also maintains a list of active stream pipelines and the components associated to them.
    /// </summary>
    internal class StreamPipelineFactory
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<StreamPipelineFactory>();
        private readonly object openCloseLock = new object();
        private bool isOpen;
        private IKafkaTransportConsumer kafkaTransportConsumer;
        private Func<string, IStreamPipeline> streamPipelineFactoryHandler;
        private readonly IStreamContextCache contextCache;
        private Action onClose = () => { };
        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        
        public event Action<IStreamPipeline[]> OnStreamsRevoked;

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
        /// Initializes a new instance of <see cref="StreamPipelineFactory"/>
        /// </summary>
        /// <param name="kafkaTransportConsumer">Transport layer to read from</param>
        /// <param name="streamPipelineFactoryHandler">Handler factory to execute for each Stream detected in the incoming messages in order to create a new <see cref="StreamPipeline"/> for each stream. 
        /// The handler function receives a StreamId and has to return a <see cref="StreamPipeline"/>.</param>
        /// <param name="contextCache">The cache to store created stream contexts</param>
        public StreamPipelineFactory(IKafkaTransportConsumer kafkaTransportConsumer, Func<string, IStreamPipeline> streamPipelineFactoryHandler, IStreamContextCache contextCache)
        {
            this.kafkaTransportConsumer = kafkaTransportConsumer ?? throw new ArgumentNullException(nameof(kafkaTransportConsumer));
            this.streamPipelineFactoryHandler = streamPipelineFactoryHandler ?? throw new ArgumentNullException(nameof(streamPipelineFactoryHandler));
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
                var consumer = this.kafkaTransportConsumer;
                if (consumer == null) return;
                this.onClose = () =>
                {
                    cancellationTokenSource.Cancel();
                };

                consumer.OnPackageReceived += this.NewTransportPackageHandler;
                this.onClose += () => { consumer.OnPackageReceived -= this.NewTransportPackageHandler; };

                consumer.OnRevoked += this.OutputRevokedHandler;
                this.onClose += () => { consumer.OnRevoked -= this.OutputRevokedHandler; };
                
                consumer.OnCommitted += this.OutputCommittedHandler;
                this.onClose += () => { consumer.OnCommitted -= this.OutputCommittedHandler; };
                
                this.isOpen = true;
            }
        }

        private void OutputCommittedHandler(object sender, CommittedEventArgs args)
        {
            lock (this.contextCache.Sync)
            {
                var streamContexts = this.contextCache.GetAll();
                var affectedStreamContexts = streamContexts.Where(kvp =>
                {
                    var lutpo = kvp.Value.LastUncommittedTopicPartitionOffset;
                    if (lutpo == null) return false;
                    var matchingTopicPartition = args.Committed.Offsets.FirstOrDefault(co => co.TopicPartition == lutpo.TopicPartition);
                    if (matchingTopicPartition == null) return false;
                    return matchingTopicPartition.Offset >= lutpo.Offset;
                }).ToList();
                    
                if (affectedStreamContexts.Count == 0) return;
                
                foreach (var kvp in affectedStreamContexts)
                {
                    kvp.Value.LastUncommittedTopicPartitionOffset = null;
                }
            }
        }

        protected virtual void OutputRevokedHandler(object sender, RevokedEventArgs args)
        {
            lock (this.contextCache.Sync)
            {
                var streamContexts = this.contextCache.GetAll();
                var affectedStreamContexts = streamContexts.Where(kvp =>
                {
                    var ltpo = kvp.Value.LastTopicPartitionOffset;
                    if (ltpo == null) return false;
                    var matchingTopicPartition = args.Revoked.FirstOrDefault(co => co.TopicPartition == ltpo.TopicPartition);
                    if (matchingTopicPartition == null) return false;
                    return matchingTopicPartition.Offset >= ltpo.Offset;
                }).ToList();
                
                if (affectedStreamContexts.Count == 0) return;

                var pipelines = affectedStreamContexts.Select(y => y.Value.StreamPipeline).ToArray();
                
                OnStreamsRevoked?.Invoke(pipelines);
                
                foreach (var affectedContext in affectedStreamContexts)
                {
                    try
                    {
                        affectedContext.Value.StreamPipeline.Close();
                    }
                    catch (Exception ex)
                    {
                        logger.LogTrace(ex, "Exception while closing stream pipeline after revocation.");
                        // Ignore fails, they may have been closed in a callback of OnStreamsRevoked
                    }
                }
            }
        }

        private Task NewTransportPackageHandler(TransportPackage package)
        {
            if (package == null)
            {
                this.logger.LogWarning("StreamPipelineFactory: Null package. Malformed package?");
                return Task.CompletedTask;
            }
            
            var streamId = package.Key;
            
            if (package.Key == null)
            {
                streamId = StreamPipeline.DefaultStreamIdWhenMissing;
            }

            StreamContext streamContext;
            lock (this.contextCache.Sync) // Locking due to updating a streamContext
            {
                if (!this.contextCache.TryGet(streamId, out streamContext))
                {
                    streamContext = new StreamContext(streamId);
                    if (!this.contextCache.TryAdd(streamContext))
                    {
                        this.logger.LogError("StreamPipelineFactory: failed to cache stream context. {0}", streamContext.StreamId);
                        return Task.CompletedTask;;
                    }

                    this.logger.LogTrace("StreamPipelineFactory: package is for a new stream");
                    // Create the new Stream pipeline with the Stream Factory handler
                    // Stream pipeline class stands for a specific Stream with its own pipeline and state (if it exists)
                    var retryCount = 0;
                    do
                    {
                        try
                        {
                            streamContext.StreamPipeline = this.streamPipelineFactoryHandler.Invoke(streamId);
                            break; // success, no retry
                        }
                        catch (Exception ex)
                        {                           
                            retryCount++;
                            if (retryCount >= this.MaxRetries && this.MaxRetries != -1)
                            {
                                this.contextCache.Remove(streamId);
                                throw new Exception($"Exception while creating a new stream pipeline for stream {streamId}. Failed {retryCount} times. Reached maximum retry count.", ex);
                            }
                            var waitFor = Math.Min(retryCount * this.RetryIncrease, this.MaxRetryDuration); 
                            this.logger.LogError(ex, "Exception while creating a new stream pipeline for stream {0}. Failed {1} times. Waiting {2}ms then retrying again.", streamId, retryCount, waitFor);
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
                    
                    // Close the stream pipeline if we received an StreamEnd message
                    this.logger.LogTrace("StreamPipelineFactory: subscribing to StreamEnd for the new stream. {0}", streamContext.StreamId);
                    streamContext.StreamPipeline.OnClosing += () =>
                    {
                        this.contextCache.Remove(streamContext.StreamId);
                        this.logger.LogTrace("StreamPipelineFactory: Removed stream from dictionary of streams {0} due to stream closing", streamContext.StreamId);
                    };
                    streamContext.StreamPipeline.Subscribe<StreamEnd>(OnStreamFinished);
                }

                streamContext.LastUncommittedTopicPartitionOffset = package.KafkaMessage?.TopicPartitionOffset;
                streamContext.LastTopicPartitionOffset = package.KafkaMessage?.TopicPartitionOffset;
            }

            // Convert Transport Package to Stream Package
            var streamPackage = new StreamPackage(package);

            // Send the Stream Package to the stream
            return streamContext.StreamPipeline.Send(streamPackage);
        }

        /// <summary>
        /// Close reading subscription from Transport layer and close all the Stream pipelines managed by the factory
        /// </summary>
        public void Close()
        {
            if (!this.isOpen) return;
            lock (this.openCloseLock)
            {
                if (!this.isOpen) return;
                this.isOpen = false;

                this.kafkaTransportConsumer = null;

                lock (this.contextCache.Sync)
                {
                    // Close the streams
                    foreach (var keyValuePair in this.contextCache.GetAll())
                    {
                        keyValuePair.Value.StreamPipeline.Close();
                    }
                }

                onClose();
            }
        }

        // Close the stream pipeline if we received an StreamEnd message
        private void OnStreamFinished(IStreamPipeline stream, StreamEnd obj)
        {
            this.logger.LogTrace("StreamPipelineFactory: OnStreamFinished -> closing stream");
            stream.Close();
            this.logger.LogTrace("StreamPipelineFactory: OnStreamFinished -> stream closed");
        }

    }
}