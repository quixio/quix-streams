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
        private readonly Func<KafkaMessage, Task> onPublish;

        public Func<KafkaMessage, Task> MessageReceived { get; set; }
        public event EventHandler<Exception> ErrorOccurred;
        public void Commit(ICollection<TopicPartitionOffset> partitionOffsets)
        {
            this.Committing?.Invoke(this, new CommittingEventArgs(partitionOffsets));
            this.Committed?.Invoke(this, new CommittedEventArgs(new CommittedOffsets(partitionOffsets.Select(y=> 
                new TopicPartitionOffsetError(y.TopicPartition, y.Offset, new Error(ErrorCode.NoError))).ToList(), new Error(ErrorCode.NoError))));    }

        public void Commit()
        {
            var offset = Interlocked.Read(ref msgCount);
            if (offset == 0) return;
            var topicPartitionOffset = CreateTopicPartitionOffset(offset);
            this.Committing?.Invoke(this, new CommittingEventArgs(new List<TopicPartitionOffset>()
            {
                topicPartitionOffset
            }));
            this.Committed?.Invoke(this, new CommittedEventArgs(new CommittedOffsets(new List<TopicPartitionOffsetError>()
            {
                new TopicPartitionOffsetError(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset, new Error(ErrorCode.NoError))
            }, new Error(ErrorCode.NoError))));
        }

        public event EventHandler<CommittedEventArgs> Committed;
        public event EventHandler<CommittingEventArgs> Committing;
        public event EventHandler<RevokingEventArgs> Revoking;
        public event EventHandler<RevokedEventArgs> Revoked;
    
        public async Task Publish(KafkaMessage message, CancellationToken cancellationToken = default)
        {
            var count = Interlocked.Increment(ref msgCount);
            var offset = CreateTopicPartitionOffset(count);
            var testMessage = new TestProducedKafkaMessage(message, offset);
            await onPublish(testMessage);
            await (this.MessageReceived?.Invoke(testMessage) ?? Task.CompletedTask);
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

        void IKafkaProducer.Open()
        {
            // do nothing;
        }

        void IKafkaProducer.Close()
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
            this.Revoking?.Invoke(this, new RevokingEventArgs(new List<TopicPartitionOffset>()
            {
                topicPartitionOffset
            }));
        
            this.Revoked?.Invoke(this, new RevokedEventArgs(new List<TopicPartitionOffset>()
            {
                topicPartitionOffset
            }));
        }
    }
    
    public class TestProducedKafkaMessage : KafkaMessage
    {
        public TestProducedKafkaMessage(KafkaMessage message, TopicPartitionOffset offset) : base(message.Key, message.Value, message.Headers, message.MessageTime, message.TopicPartitionOffset)
        {
            this.TopicPartitionOffset = offset;
            this.MessageTime = message.MessageTime.Type == KafkaMessageTimeType.ProduceTime
                ? new KafkaMessageTime(DateTime.UtcNow, KafkaMessageTimeType.CreateTime)
                : message.MessageTime;
        }
    }
}