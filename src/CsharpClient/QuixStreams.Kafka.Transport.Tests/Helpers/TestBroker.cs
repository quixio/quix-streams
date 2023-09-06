using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QuixStreams.Kafka.Transport.Tests.Helpers
{
    public class TestBroker : IKafkaConsumer, IKafkaProducer
    {

        public TestBroker(Func<KafkaMessage, Task> onPublish = null)
        {
            this.onPublish = onPublish ?? (message => { return Task.CompletedTask;});
        }
        
        public void Dispose()
        {
            // do nothing
        }

        private long msgCount = 0;
        public long MessageCount => msgCount;
        private readonly Func<KafkaMessage, Task> onPublish;

        public Func<KafkaMessage, Task> OnMessageReceived { get; set; }
        public event EventHandler<Exception> OnErrorOccurred;
        public void Commit(ICollection<TopicPartitionOffset> partitionOffsets)
        {
            this.OnCommitting?.Invoke(this, new CommittingEventArgs(partitionOffsets));
            this.OnCommitted?.Invoke(this, new CommittedEventArgs(new CommittedOffsets(partitionOffsets.Select(y=> 
                new TopicPartitionOffsetError(y.TopicPartition, y.Offset, new Error(ErrorCode.NoError))).ToList(), new Error(ErrorCode.NoError))));    }

        public void Commit()
        {
            var offset = Interlocked.Read(ref msgCount);
            if (offset == 0) return;
            var topicPartitionOffset = CreateTopicPartitionOffset(offset);
            this.OnCommitting?.Invoke(this, new CommittingEventArgs(new List<TopicPartitionOffset>()
            {
                topicPartitionOffset
            }));
            this.OnCommitted?.Invoke(this, new CommittedEventArgs(new CommittedOffsets(new List<TopicPartitionOffsetError>()
            {
                new TopicPartitionOffsetError(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset, new Error(ErrorCode.NoError))
            }, new Error(ErrorCode.NoError))));
        }

        public event EventHandler<CommittedEventArgs> OnCommitted;
        public event EventHandler<CommittingEventArgs> OnCommitting;
        public event EventHandler<RevokingEventArgs> OnRevoking;
        public event EventHandler<RevokedEventArgs> OnRevoked;
    
        public async Task Publish(KafkaMessage message, CancellationToken cancellationToken = default)
        {
            var count = Interlocked.Increment(ref msgCount);
            var offset = CreateTopicPartitionOffset(count);
            var testMessage = new TestProducedKafkaMessage(message, offset);
            await onPublish(testMessage);
            await (this.OnMessageReceived?.Invoke(testMessage) ?? Task.CompletedTask);
        }

        private TopicPartitionOffset CreateTopicPartitionOffset(long index)
        {
            return new TopicPartitionOffset(new TopicPartition("TestTopic", 0), new Offset(index));
        }

        public async Task Publish(IEnumerable<KafkaMessage> messages, CancellationToken cancellationToken = default)
        {
            foreach (var kafkaMessage in messages)
            {
                await this.Publish(kafkaMessage, cancellationToken);
            }
        }

        public void Flush(CancellationToken cancellationToken)
        {
            // do nothing;
        }

        public int MaxMessageSizeBytes { get; set; } = 1024 * 1024;

        void IKafkaConsumer.Open()
        {
            // do nothing;
        }

        void IKafkaConsumer.Close()
        {
            // do nothing
        }

        /// <summary>
        /// Helper method
        /// </summary>
        public void Revoke()
        {
            var offset = Interlocked.Read(ref msgCount);
            var topicPartitionOffset = CreateTopicPartitionOffset(offset);
            this.OnRevoking?.Invoke(this, new RevokingEventArgs(new List<TopicPartitionOffset>()
            {
                topicPartitionOffset
            }));
        
            this.OnRevoked?.Invoke(this, new RevokedEventArgs(new List<TopicPartitionOffset>()
            {
                topicPartitionOffset
            }));
        }
    }
    
    public class TestProducedKafkaMessage : KafkaMessage
    {
        public TestProducedKafkaMessage(KafkaMessage message, TopicPartitionOffset offset) : base(message.Key, message.Value, message.Headers, message.Timestamp, message.TopicPartitionOffset)
        {
            this.TopicPartitionOffset = offset;
            this.Timestamp = message.Timestamp;
        }
    }
}