using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Confluent.Kafka;

namespace QuixStreams.Kafka
{
    /// <summary>
    /// Topic configuration for Kafka producer
    /// </summary>
    public sealed class ProducerTopicConfiguration
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ProducerTopicConfiguration"/>
        /// </summary>
        /// <param name="topic">The topic to write to</param>
        /// <param name="partition">The partition to write to</param>
        public ProducerTopicConfiguration(string topic, Partition partition)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentOutOfRangeException(nameof(topic), "Cannot be null or empty");
            }

            this.Topic = topic;
            this.Partition = partition;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ProducerTopicConfiguration"/>
        /// </summary>
        /// <param name="topic">The topic to write to</param>
        public ProducerTopicConfiguration(string topic) : this(topic, Partition.Any)
        {
        }

        /// <summary>
        /// The topic to write to
        /// </summary>
        public string Topic { get; }

        /// <summary>
        /// The partition to write to
        /// </summary>
        public Partition Partition { get; }
    }

    public sealed class ConsumerTopicConfiguration
    {
        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> for a single topic where partitions will be automatically 
        /// selected and offset will be the last unread offset or first available offset if
        /// no previous offset for consumer group is found.
        /// </summary>
        /// <param name="topic">The topic</param>
        public ConsumerTopicConfiguration(string topic)
        {
            this.Topics = new ReadOnlyCollection<string>(new List<string>(1) {topic});
        }

        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> for one or more topics where partitions will be automatically 
        /// selected and offset will be the last unread offset or first available
        /// offset if no previous offset for consumer group is found.
        /// </summary>
        /// <param name="topics">The topics</param>
        public ConsumerTopicConfiguration(ICollection<string> topics)
        {
            this.Topics = new ReadOnlyCollection<string>(topics.ToList());
        }

        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with a single topic and partition with default offset. 
        /// Default is the last unread offset or first available offset if
        /// no previous offset for consumer group is found.
        /// </summary>
        /// <param name="topic">The topic to set the partitions for</param>
        /// <param name="partition">The partition to set the offset to default for</param>
        public ConsumerTopicConfiguration(string topic, Partition partition) : this(topic, new List<Partition>(1) {partition}, Offset.Unset)
        {
        }
        
        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with a single topic and offset with default partition.
        /// </summary>
        /// <param name="topic">The topic to set the partitions for</param>
        /// <param name="offset">The offset to use</param>
        public ConsumerTopicConfiguration(string topic, Offset offset) : this(topic, new List<Partition>(1) {Partition.Any}, offset)
        {
        }        

        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with a single topic and partition with the specified offset.
        /// </summary>
        /// <param name="topic">The topic to set the partitions for</param>
        /// <param name="partition">The partition to set the offset for</param>
        /// <param name="offset">The offset</param>
        public ConsumerTopicConfiguration(string topic, Partition partition, Offset offset) : this(topic, new List<Partition>(1) {partition}, offset)
        {
        }

        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with a single topic for, which all specified partitions are set to default. 
        /// Default is the last unread offset or first available offset if
        /// no previous offset for consumer group is found.
        /// </summary>
        /// <param name="topic">The topic to set the partitions for</param>
        /// <param name="partitions">The partitions to set the offset to default for</param>
        public ConsumerTopicConfiguration(string topic, ICollection<Partition> partitions) : this(topic, partitions, Offset.Unset)
        {
        }


        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with multiple topics where each topic has one or more configured partition offset
        /// </summary>
        /// <param name="topicPartitionOffsets">The topics with partition offsets</param>
        public ConsumerTopicConfiguration(ICollection<TopicPartitionOffset> topicPartitionOffsets)
        {
            if (topicPartitionOffsets == null)
            {
                throw new ArgumentNullException(nameof(topicPartitionOffsets));
            }

            if (topicPartitionOffsets.Count == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(topicPartitionOffsets), "Cannot be empty");
            }

            var groupedTopicPartitions = topicPartitionOffsets.GroupBy(tpo => tpo.Topic).ToDictionary(x => x.Key, x => x.ToList());

            foreach (var partitionOffsets in groupedTopicPartitions)
            {
                if (partitionOffsets.Value.Any(x => x.Offset.Value.Equals(Partition.Any)) && partitionOffsets.Value.Count != 1)
                {
                    throw new ArgumentOutOfRangeException(nameof(partitionOffsets),
                        $"Provided multiple partition values for {partitionOffsets.Key} where at least one is Any. Should provide only Any or multiple without Any.");
                }
            }

            // check if it is a simple subscription to topics
            if (topicPartitionOffsets.All(p => p.Partition == Partition.Any && p.Offset == Offset.Unset))
            {
                this.Topics = new ReadOnlyCollection<string>(topicPartitionOffsets.Select(x => x.Topic).Distinct().ToList());
                return;
            }
 
            // no, it is not a simple topic subscription
            this.Partitions = new ReadOnlyCollection<TopicPartitionOffset>(topicPartitionOffsets.ToList());
        }

        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with a single topic for, which all partitions set to one type of offset.
        /// </summary>
        /// <param name="topic">The topic to set the partitions for</param>
        /// <param name="partitions">The partitions to set the offset for</param>
        /// <param name="offset">The offset</param>
        public ConsumerTopicConfiguration(string topic, ICollection<Partition> partitions, Offset offset) : this(topic, partitions.Select(p => new PartitionOffset(p.Value, offset)).ToList())
        {
        }

        /// <summary>
        /// Initializes a new <see cref="ConsumerTopicConfiguration" /> with a single topic with the specified partition offsets.
        /// </summary>
        /// <param name="topic">The topic to set the partitions for</param>
        /// <param name="partitionOffsets">The partitions with offsets to listen to</param>
        public ConsumerTopicConfiguration(string topic, ICollection<PartitionOffset> partitionOffsets) : this(partitionOffsets.Select(x=> new TopicPartitionOffset(topic, x.Partition, x.Offset)).ToList())
        {
        }

        /// <summary>
        /// The topics
        /// </summary>
        public IReadOnlyCollection<string> Topics { get; }

        /// <summary>
        /// The topics with partition offsets
        /// </summary>
        public IReadOnlyCollection<TopicPartitionOffset> Partitions { get; }
    }

    /// <summary>
    /// The offset to use for a given partition
    /// </summary>
    public class PartitionOffset
    {
        /// <summary>
        /// Initializes a new instance of <see cref="PartitionOffset"/>
        /// </summary>
        /// <param name="partition">The partition</param>
        /// <param name="offset">The offset</param>
        public PartitionOffset(Partition partition, Offset offset)
        {
            this.Partition = partition;
            this.Offset = offset;
        }

        /// <summary>
        /// The partition
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        /// The Offset
        /// </summary>
        public Offset Offset { get; }
    }
}