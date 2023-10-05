using System;

namespace QuixStreams.Streaming.Models
{
    public class StreamConsumerId
    {
        public string ConsumerGroup { get; }
        public string TopicName { get; }
        public int Partition { get; }
        public string StreamId { get; }

        /// <summary>
        /// Represents a unique identifier for a stream consumer.
        /// <param name="consumerGroup">Name of the consumer group.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="partition">Topic partition number.</param>
        /// <param name="streamId">Stream Id of the source that has generated this Stream Consumer.</param>
        /// Commonly the Stream Id will be coming from the protocol. 
        /// If no stream Id is passed, like when a new stream is created for producing data, a Guid is generated automatically.
        /// </summary>
        public StreamConsumerId(string consumerGroup, string topicName, int partition, string streamId)
        {
            ConsumerGroup = consumerGroup;
            TopicName = topicName;
            Partition = partition;
            StreamId = streamId;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (obj is StreamConsumerId other)
            {
                return ConsumerGroup == other.ConsumerGroup &&
                       TopicName == other.TopicName &&
                       Partition == other.Partition &&
                       StreamId == other.StreamId;
            }

            return false;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(ConsumerGroup, TopicName, Partition, StreamId);
        }
    }
}