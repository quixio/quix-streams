using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Kafka
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
        private IDictionary<string, string> brokerStates = new ConcurrentDictionary<string, string>();
        private bool checkBrokerStateBeforeSend = false;
        private bool logOnNextBrokerStateUp = false;
        private bool disableKafkaLogsByBrokerLogWorkaround = false; // if enabled, no actual kafka logs should be shown


        private readonly ProduceDelegate produce;

        private long lastFlush = -1;
        private IProducer<byte[], byte[]> producer;
        private string configId;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaProducer"/>
        /// </summary>
        /// <param name="publisherConfiguration">The publisher configuration</param>
        /// <param name="topicConfiguration">The topic configuration</param>
        public KafkaProducer(PublisherConfiguration publisherConfiguration, ProducerTopicConfiguration topicConfiguration)
        {
            this.config = this.GetKafkaProducerConfig(publisherConfiguration);
            SetConfigId(topicConfiguration);
            if (topicConfiguration.Partition == Partition.Any)
            {
                this.produce = (key, value, handler, _) => this.producer.Produce(topicConfiguration.Topic, new Message<byte[], byte[]> { Key = key, Value = value }, handler);
            }
            else
            {
                var topicPartition = new TopicPartition(topicConfiguration.Topic, topicConfiguration.Partition);
                this.produce = (key, value, handler, _) => this.producer.Produce(topicPartition, new Message<byte[], byte[]> { Key = key, Value = value }, handler);
            }
        }
        
        private ProducerConfig GetKafkaProducerConfig(PublisherConfiguration publisherConfiguration)
        {
            var config = publisherConfiguration.ToProducerConfig();
            config.Debug = config.Debug;
            if (!string.IsNullOrWhiteSpace(config.Debug))
            {
                if (config.Debug.Contains("all")) return config;
                if (config.Debug.Contains("broker")) return config;
                // There is a debug configuration other than all or queue
                this.logger.LogDebug("In order to enable a workaround to check if broker is up, additional broker logs will be visible");
                config.Debug = (config.Debug.TrimEnd(new[] { ',', ' ' }) + ",broker").TrimStart(',');
                return config;
            }

            disableKafkaLogsByBrokerLogWorkaround = true;
            config.Debug = "broker";

            return config;
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

                this.producer = new ProducerBuilder<byte[], byte[]>(this.config)
                    .SetErrorHandler(this.ErrorHandler)
                    .SetLogHandler(this.ProducerLogHandler)
                    .Build();
            }
        }
        
        private void ProducerLogHandler(IProducer<byte[], byte[]> consumer, LogMessage msg)
        {
            if (KafkaHelper.TryParseBrokerNameChange(msg, out var oldName, out var newName))
            {
                if (brokerStates.ContainsKey(oldName))
                {
                    brokerStates[newName] = brokerStates[oldName];
                    brokerStates.Remove(oldName);
                    if (disableKafkaLogsByBrokerLogWorkaround) this.logger.LogTrace("[{0}] Broker {1} is now {2}", this.configId, oldName, newName);
                }
            }
            
            if (KafkaHelper.TryParseBrokerState(msg, out var broker, out var state))
            {
                if (logOnNextBrokerStateUp && state.Equals("up", StringComparison.InvariantCultureIgnoreCase))
                {
                    this.logger.LogInformation("[{0}] Broker {1} is now {2}", this.configId, broker, state);
                } 
                brokerStates[broker] = state;
            }
            
            if (disableKafkaLogsByBrokerLogWorkaround) return;
            
            switch (msg.Level)
            {
                case SyslogLevel.Alert:
                case SyslogLevel.Warning:
                    logger.LogWarning("[{0}][Kafka log][{1}] {2}", this.configId, msg.Facility, msg.Message);
                    break;
                case SyslogLevel.Emergency:
                case SyslogLevel.Critical:
                    logger.LogCritical("[{0}][Kafka log][{1}] {2}", this.configId, msg.Facility, msg.Message);
                    break;
                case SyslogLevel.Error:
                    logger.LogError("[{0}][Kafka log][{1}] {2}", this.configId, msg.Facility, msg.Message);
                    break;
                case SyslogLevel.Notice:
                case SyslogLevel.Info:
                    logger.LogInformation("[{0}][Kafka log][{1}] {2}", this.configId, msg.Facility, msg.Message);
                    break;
                case SyslogLevel.Debug:
                    logger.LogDebug("[{0}][Kafka log][{1}] {2}", this.configId, msg.Facility, msg.Message);
                    break;
                default:
                    logger.LogDebug("[{0}][Kafka log][{1}] {2}", this.configId, msg.Facility, msg.Message);
                    break;
            } 
        }

        private void ErrorHandler(IProducer<byte[], byte[]> producer, Error error)
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

            if (ex.Message.Contains("brokers are down"))
            {
                checkBrokerStateBeforeSend = true;
                this.logger.LogDebug("[{0}] {1}, but delaying reporting until next message, in case reconnect happens before.", this.configId, ex.Message); // Excessive error reporting
                return;
            }
            
            if (ex.Message.Contains("Receive failed") && ex.Message.Contains("Connection timed out (after "))
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
                return Task.FromCanceled<DeliveryResult<byte[], byte[]>>(cancellationToken);
            }

            var taskSource = new TaskCompletionSource<DeliveryResult<byte[], byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);

            void DeliveryHandler(DeliveryReport<byte[], byte[]> report)
            {
                if (report.Error?.IsError == true)
                {
                    this.logger.LogTrace("[{0}] {1} {2}", this.configId, report.Error.Code, report.Error.Reason);
                    var wrappedError = new Error(report.Error.Code, $"[{this.configId}] {report.Error.Reason}", report.Error.IsFatal);
                    taskSource.SetException(new ProduceException<byte[], byte[]>(wrappedError, report));
                    return;
                }

                taskSource.SetResult(report);
            }

            var success = false;
            var maxTry = 10;
            var tryCount = 0;
            lock (sendLock) // to avoid reordering of packages in case of error
            {
                if (checkBrokerStateBeforeSend)
                {
                    checkBrokerStateBeforeSend = false;
                    var upBrokerCount = this.brokerStates.Count(y => y.Value.Equals("up", StringComparison.InvariantCultureIgnoreCase));
                    if (upBrokerCount == 0)
                    {
                        logOnNextBrokerStateUp = true;
                        this.logger.LogError("[{0}] None of the brokers are currently in state 'up'.", this.configId);
                        if (this.logger.IsEnabled(LogLevel.Debug))
                        {
                            foreach (var brokerState in brokerStates)
                            {
                                this.logger.LogDebug("[{0}] Broker {1} has state {2}", this.configId, brokerState.Key, brokerState.Value);
                            }
                        }
                    }
                    else
                    {
                        this.logger.LogDebug("[{0}] {1}/{2} brokers are up (after all being marked down)", this.configId, upBrokerCount, this.brokerStates.Count);
                    }
                } 
                do
                {
                    try
                    {
                        tryCount++;
                        var key = package.GetKey();
                        var byteArray = (byte[]) package.Value;
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
        }

        private delegate void ProduceDelegate(byte[] key, byte[] message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler, object state);
    }
}