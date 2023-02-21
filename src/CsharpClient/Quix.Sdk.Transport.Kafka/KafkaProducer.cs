using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Transport.IO;
using Serilog.Core;

namespace Quix.Sdk.Transport.Kafka
{
    /// <summary>
    /// Kafka producer implemented using Queueing mechanism
    /// </summary>
    public class KafkaProducer : IKafkaProducer
    {
        private readonly ProducerConfig config;

        private readonly object flushLock = new object();
        private readonly object sendLock = new object();

        private readonly object openLock = new object();
        private readonly ILogger logger = Logging.CreateLogger<KafkaProducer>();

        private readonly ProduceDelegate produce;

        private long lastFlush = -1;
        private IProducer<string, byte[]> producer;
        private readonly ThreadingTimer timer;
        private List<TopicPartition> partitionsToKeepAliveWith = new List<TopicPartition>();
        private readonly Action setupKeepAlive;
        private string configId;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaProducer"/>
        /// </summary>
        /// <param name="publisherConfiguration">The publisher configuration</param>
        /// <param name="topicConfiguration">The topic configuration</param>
        public KafkaProducer(PublisherConfiguration publisherConfiguration, ProducerTopicConfiguration topicConfiguration)
        {
            Action hbAction = null;
            this.config = publisherConfiguration.ToProducerConfig();
            SetConfigId(topicConfiguration);
            if (topicConfiguration.Partition == Partition.Any)
            {
                this.produce = (key, value, handler, _) => this.producer.Produce(topicConfiguration.Topic, new Message<string, byte[]> { Key = key, Value = value }, handler);
                hbAction = () =>
                {
                    this.logger.LogTrace("[{0}] Creating admin client to retrieve metadata for keep alive details", this.configId);
                    try
                    {
                        using (var adminClient = new AdminClientBuilder(this.config).Build())
                        {
                            var metadata = adminClient.GetMetadata(topicConfiguration.Topic, TimeSpan.FromSeconds(15));
                            if (metadata == null)
                            {
                                logger.LogError("[{0}] Keep alive could not be set up for topic {1} due to timeout.", this.configId, topicConfiguration.Topic);
                                return;
                            }

                            this.logger.LogTrace("[{0}] Retrieved metadata for Topic {1}", this.configId, topicConfiguration.Topic);
                            var topicMetaData = metadata.Topics.FirstOrDefault(x => x.Topic == topicConfiguration.Topic);
                            if (topicMetaData == null)
                            {
                                logger.LogError("[{0}] Keep alive could not be set up for topic {1} due to missing topic metadata.", this.configId, topicConfiguration.Topic);
                                return;
                            }

                            partitionsToKeepAliveWith = topicMetaData.Partitions.GroupBy(y => y.Leader)
                                .Select(y => y.First())
                                .Select(y => new TopicPartition(topicConfiguration.Topic, y.PartitionId)).ToList();

                            if (logger.IsEnabled(LogLevel.Debug))
                            {
                                foreach (var topicPartition in partitionsToKeepAliveWith)
                                {
                                    logger.LogDebug("[{0}] Automatic keep alive enabled for '{1}'.", this.configId, topicPartition);
                                }
                            }
                        }

                        this.logger.LogTrace("[{0}] Finished retrieving metadata for keep alive details", this.configId);
                    }
                    catch (KafkaException kafkaException)
                    {
                        logger.LogError("[{0}] Keep alive could not be set up for topic {1} due to error: {2}.", this.configId, topicConfiguration.Topic, kafkaException.Message);
                        return;
                    }

                    hbAction = () => { }; // because no need to do ever again, really
                };
            }
            else
            {
                var topicPartition = new TopicPartition(topicConfiguration.Topic, topicConfiguration.Partition);
                this.produce = (key, value, handler, _) => this.producer.Produce(topicPartition, new Message<string, byte[]> { Key = key, Value = value }, handler);
                hbAction = () =>
                {
                    partitionsToKeepAliveWith.Add(topicPartition);
                    hbAction = () => { }; // because no need to do ever again, really
                };
            }

            if (publisherConfiguration.KeepConnectionAlive && publisherConfiguration.KeepConnectionAliveInterval > 0)
            {
                setupKeepAlive = hbAction;
                this.timer = new ThreadingTimer(SendKeepAlive, publisherConfiguration.KeepConnectionAliveInterval, this.logger);
            }
        }
        
        private void SetConfigId(ProducerTopicConfiguration topicConfiguration)
        {
            this.configId = Guid.NewGuid().GetHashCode().ToString("X8");
            var logBuilder = new StringBuilder();
            logBuilder.AppendLine();
            logBuilder.AppendLine("=================== Kafka Producer Configuration =====================");
            logBuilder.AppendLine("= Configuration Id: " + this.configId);
            logBuilder.AppendLine($"= Topic: {topicConfiguration.Topic}{topicConfiguration.Partition}");
            foreach (var keyValuePair in this.config)
            {
                if (keyValuePair.Key?.IndexOf("password", StringComparison.InvariantCultureIgnoreCase) > -1 ||
                    keyValuePair.Key?.IndexOf("username", StringComparison.InvariantCultureIgnoreCase) > -1)
                {
                    logBuilder.AppendLine($"= {keyValuePair.Key}: [REDACTED]");
                }
                else logBuilder.AppendLine($"= {keyValuePair.Key}: {keyValuePair.Value}");
            }
            logBuilder.Append("======================================================================");
            this.logger.LogDebug(logBuilder.ToString());
        }

        /// <inheritdoc />
        public void Open()
        {
            if (this.producer != null) return;
            lock (this.openLock)
            {
                if (this.producer != null) return;

                this.producer = new ProducerBuilder<string, byte[]>(this.config)
                    .SetErrorHandler(this.ErrorHandler)
                    .Build();

                if (setupKeepAlive != null)
                {
                    setupKeepAlive();
                }
                this.timer?.Start();
            }
        }

        private void SendKeepAlive()
        {
            void ProduceKeepAlive(string key, byte[] message, Action<DeliveryReport<string, byte[]>> deliveryHandler, object state)
            {
                var topicPartition = (TopicPartition) state;
                this.producer.Produce(topicPartition, new Message<string, byte[]> { Key = key, Value = message }, deliveryHandler);
            }
            
            try
            {
                foreach (var topicPartition in partitionsToKeepAliveWith)
                {
                    logger.LogTrace("[{0}] Sending keep alive msg to {1}", this.configId, topicPartition);
                    SendInternal(Constants.KeepAlivePackage, ProduceKeepAlive, state: topicPartition).GetAwaiter().GetResult();
                    logger.LogTrace("[{0}] Sent keep alive msg to {1}", this.configId, topicPartition);
                }
            }
            catch
            {
                logger.LogWarning("[{0}] Failed to send keep alive msg", this.configId);    
            }
        }

        private void ErrorHandler(IProducer<string, byte[]> producer, Error error)
        {
            // TODO possibly allow delegation of error up
            var ex = new KafkaException(error);
            if (ex.Message.ToLowerInvariant().Contains("disconnect"))
            {
                var match = Constants.ExceptionMsRegex.Match(ex.Message);
                if (match.Success)
                {
                    if (int.TryParse(match.Groups[1].Value, out var ms))
                    {
                        if (ms > 180000)
                        {
                            this.logger.LogDebug(ex, "[{0}] Idle producer connection reaped.", this.configId);
                            return;
                        }
                    }
                }
                this.logger.LogWarning(ex, "[{0}] Disconnected from kafka. Ignore unless occurs frequently in short period of time as client automatically reconnects.", this.configId);
                return;
            }
            
            if (ex.Message.Contains("Receive failed: Connection timed out (after "))
            {
                var match = Constants.ExceptionMsRegex.Match(ex.Message);
                if (match.Success)
                {
                    if (int.TryParse(match.Groups[1].Value, out var ms))
                    {
                        if (ms > 7500000)
                        {
                            this.logger.LogInformation(ex, "[{0}] Idle producer connection timed out, Kafka will reconnect.", this.configId);
                            return;
                        }
                        this.logger.LogWarning(ex, "[{0}] Producer connection timed out (after {1}ms in state UP). Kafka will reconnect.", this.configId, ms);
                        return;
                    }
                }
            }
            
            this.logger.LogError(ex, "[{0}] Kafka producer exception", this.configId);
        }

        /// <inheritdoc />
        public void Close()
        {
            if (this.producer == null) return;
            lock (this.openLock)
            {
                if (this.producer == null) return;

                this.producer.Dispose();
                this.producer = null;
                this.timer?.Stop();
            }
        }

        /// <summary>
        /// Send a package to the configured kafka.
        /// </summary>
        /// <param name="package">The package to send</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting send</param>
        /// <returns>An awaitable <see cref="Task"/></returns>
        public Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            return this.SendInternal(package, this.produce, cancellationToken);
        }


        private Task SendInternal(Package package, ProduceDelegate handler,  CancellationToken cancellationToken = default, object state = null)
        {
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
            if (this.producer == null)
            {
                lock (this.openLock)
                {
                    if (this.producer == null)
                    {
                        throw new InvalidOperationException($"[{this.configId}] Unable to write while producer is closed");
                    }
                }
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<DeliveryResult<string, byte[]>>(cancellationToken);
            }

            var taskSource = new TaskCompletionSource<DeliveryResult<string, byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);

            void DeliveryHandler(DeliveryReport<string, byte[]> report)
            {
                if (report.Error?.IsError == true)
                {
                    var wrappedError = new Error(report.Error.Code, $"[{this.configId}] {report.Error.Reason}", report.Error.IsFatal);
                    taskSource.SetException(new ProduceException<string, byte[]>(wrappedError, report));
                    return;
                }

                taskSource.SetResult(report);
            }

            var success = false;
            var maxTry = 10;
            var tryCount = 0;
            lock (sendLock) // to avoid reordering of packages in case of error
            {
                do
                {
                    try
                    {
                        tryCount++;
                        var key = package.GetKey();
                        var byteArray = (byte[]) package.Value.Value;
                        handler(key, byteArray, DeliveryHandler, state);
                        success = true;
                    }
                    catch (ProduceException<byte[], byte[]> e)
                    {
                        if (tryCount == maxTry) throw;

                        if (e.Error.Code == ErrorCode.Local_QueueFull)
                        {
                            this.Flush(cancellationToken);
                        }
                        else
                        {
                            throw;
                        }
                    }
                } while (!success && tryCount <= maxTry);
            }

            return taskSource.Task;
        }

        /// <inheritdoc />
        public void Flush(CancellationToken cancellationToken)
        {
            if (this.producer == null) throw new InvalidOperationException($"[{this.configId}] Unable to flush a closed " + nameof(KafkaProducer));
            var flushTime = DateTime.UtcNow.ToBinary();
            if (flushTime < this.lastFlush) return;

            lock (this.flushLock)
            {
                if (flushTime < this.lastFlush) return;

                try
                {
                    this.producer.Flush(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // someone cancelled token
                }
                catch (ObjectDisposedException)
                {
                    // the underlying producer is disposed
                }

                this.lastFlush = DateTime.UtcNow.ToBinary();
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.Close();
            if(this.timer != null)
            {
                this.timer.Dispose();
            }
        }

        private delegate void ProduceDelegate(string key, byte[] message, Action<DeliveryReport<string, byte[]>> deliveryHandler, object state);
    }
}