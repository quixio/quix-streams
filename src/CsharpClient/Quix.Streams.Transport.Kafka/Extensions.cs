using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Transport.Kafka
{
    public static class KafkaConsumerExtensions
    {
        private static Lazy<ILogger> logger = new Lazy<ILogger>(() => Logging.CreateLogger(typeof(KafkaConsumerExtensions)));
        
        /// <summary>
        /// Commit a transport contexts limited by the topics this consumer had previously subscribed to
        /// </summary>
        /// <param name="kafkaConsumer">The consumer to commit to</param>
        /// <param name="transportContext">The transport contexts to commit</param>
        public static void CommitOffset(this IKafkaConsumer kafkaConsumer, TransportContext transportContext)
        {
            kafkaConsumer.CommitOffsets(new [] {transportContext});
        }

        /// <summary>
        /// Commit a list of transport contexts limited by the topics this consumer had previously subscribed to
        /// </summary>
        /// <param name="kafkaConsumer">The consumer to commit to</param>
        /// <param name="transportContexts">The transport contexts to commit</param>
        public static void CommitOffsets(this IKafkaConsumer kafkaConsumer, TransportContext[] transportContexts)
        {
            if (transportContexts.Length == 0)
            {
                return;
            }

            const int offSetToNextPackage = 1; // without this, we would always read back the very last one
            if (transportContexts.Length == 1)
            {
                var transportContext = transportContexts[0];
                if (!transportContext.TryGetKafkaCommitDetails(out var topic, out var partition, out var offset)) return;
                kafkaConsumer.CommitOffset(new TopicPartitionOffset(topic, partition, offset + offSetToNextPackage));
                return;
            }

            var offsets = GetPartitionOffsets(transportContexts, false);
            
            var latestOffsets = offsets.GroupBy(x => x.TopicPartition)
                .ToDictionary(x => x.Key, x => x.Max(y => y.Offset.Value) + offSetToNextPackage).Select(y =>
                    new TopicPartitionOffset(y.Key, y.Value)).ToList();

            if (latestOffsets.Count == 0) return;
            
            kafkaConsumer.CommitOffsets(latestOffsets);
        }

        /// <summary>
        /// Converts the transport contexts to <see cref="TopicPartitionOffset"/>
        /// </summary>
        /// <param name="transportContexts"></param>
        /// <param name="includeInvalidAsNull"></param>
        /// <returns>Converted in order</returns>
        internal static IEnumerable<TopicPartitionOffset> GetPartitionOffsets(TransportContext[] transportContexts, bool includeInvalidAsNull)
        {
            for (var index = 0; index < transportContexts.Length; index++)
            {
                var transportContext = transportContexts[index];
                if (transportContext == null) // This should no longer
                {
                    logger.Value.LogDebug("Transport context {0} of {1} is null during execution of GetPartitionOffsets.", index, transportContexts.Length);
                    if (includeInvalidAsNull) yield return null;
                    continue;
                };
                if (!transportContext.TryGetKafkaCommitDetails(out var topic, out var partition, out var offset))
                {
                    if (includeInvalidAsNull) yield return null;
                    continue;
                }
                yield return new TopicPartitionOffset(topic, partition, offset);
            }
        }

        /// <summary>
        /// Retrieves kafka commit details from the specified transport context
        /// </summary>
        /// <param name="transportContext">The transport context</param>
        /// <param name="topic">Topic name</param>
        /// <param name="partition">Partition id within topic</param>
        /// <param name="offset">Offset within topic and partition</param>
        /// <returns>Whether the details could be retrieved</returns>
        /// <exception cref="ArgumentNullException">If transportContext is null</exception>
        /// <exception cref="InvalidOperationException">When context information is present but is invalid</exception>
        public static bool TryGetKafkaCommitDetails(this TransportContext transportContext, out string topic, out int partition,
            out long offset)
        {
            topic = null;
            partition = 0;
            offset = 0;
            if (transportContext == null) throw new ArgumentNullException(nameof(transportContext));
            if (!transportContext.TryGetValue(KnownKafkaTransportContextKeys.Topic, out var topicObj))
            {
                return false;
            }

            if (!(topicObj is string topicVal))
            {
                throw new InvalidOperationException($"Topic found in transport context is invalid. Verify your context. Topic found: {topicObj}");
            }

            topic = topicVal;
            
            if (!transportContext.TryGetValue(KnownKafkaTransportContextKeys.Offset, out var offsetObj))
            {
                return false;
            }
            
            if (!(offsetObj is long offsetVal))
            {
                throw new InvalidOperationException($"Offset found in transport context is invalid. Verify your context. Offset found: {offsetObj}");
            }
            offset = offsetVal;
            
            if (!transportContext.TryGetValue(KnownKafkaTransportContextKeys.Partition, out var partitionObj))
            {
                return false;
            }

            if (!(partitionObj is int partitionVal))
            {
                throw new InvalidOperationException($"Offset found in transport context is invalid. Verify your context. Partition found: {partitionObj}");
            }

            partition = partitionVal;
            return true;
        }

        /// <summary>
        /// Commit an offset limited by the topics this consumer had previously subscribed to
        /// </summary>
        /// <param name="kafkaConsumer">The kafka consumer</param>
        /// <param name="offset">The offset to commit</param>
        public static void CommitOffset(this IKafkaConsumer kafkaConsumer, TopicPartitionOffset offset)
        {
            kafkaConsumer.CommitOffsets(new[]{ offset });
        }
    }
    
    public static class TransportContextExtensions
    {
        /// <summary>
        /// Retrieves the Kafka key from the package according to the Kafka protocol
        /// </summary>
        /// <param name="transportContext">The transport context to retrieve the key from</param>
        /// <returns>The key if found, else null</returns>
        public static string GetKey(this TransportContext transportContext)
        {
            if (transportContext == null) return null;
            if (!transportContext.TryGetValue(KnownKafkaTransportContextKeys.Key, out var key))
            {
                return null;
            }

            return (string)key;
        }

        /// <summary>
        /// Sets the Kafka key for the package according to the Kafka protocol
        /// </summary>
        /// <param name="transportContext">The transport context to set the key for</param>
        /// <param name="key">The kafka key</param>
        public static void SetKey(this TransportContext transportContext, string key)
        {
            transportContext[KnownKafkaTransportContextKeys.Key] = key;
        }
    }
    
    public static class PackageExtensions
    {
        /// <summary>
        /// Retrieves the Kafka key from the package according to the Kafka protocol
        /// </summary>
        /// <param name="package">The package to retrieve the key from</param>
        /// <returns>The key if found, else null</returns>
        public static string GetKey(this Package package)
        {
            return  package.TransportContext.GetKey();
        }

        /// <summary>
        /// Sets the Kafka key for the package according to the Kafka protocol
        /// </summary>
        /// <param name="package">The package to set the key for</param>
        /// <param name="key">The kafka key</param>
        public static void SetKey(this Package package, string key)
        {
            package.TransportContext.SetKey(key);
        }

        internal static bool IsKeepAlivePackage(this Package package)
        {
            var key = package.GetKey();
            return key != null && key.Equals(Constants.KeepAlivePackage.GetKey());
        }
    }
}