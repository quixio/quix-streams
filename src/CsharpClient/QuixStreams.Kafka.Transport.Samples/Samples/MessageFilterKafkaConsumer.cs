using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QuixStreams.Kafka.Transport.Samples.Samples
{
    public delegate bool MessageFilter(KafkaMessage message);

    public class MessageFilterKafkaConsumer : IKafkaConsumer
    {
        private readonly MessageFilter filter;
        private readonly IKafkaConsumer kafkaConsumer;

        /// <summary>
        /// Initializes new instance of <see cref="MessageFilterKafkaConsumer" />
        /// </summary>
        /// <param name="kafkaConsumer"></param>
        /// <param name="filter">Filter to use. When filter returns true, message is kept</param>
        public MessageFilterKafkaConsumer(IKafkaConsumer kafkaConsumer, MessageFilter filter)
        {
            this.filter = filter;
            this.kafkaConsumer = kafkaConsumer;
            kafkaConsumer.OnMessageReceived = FilterNewPackage;
            kafkaConsumer.OnCommitted += (s, e) =>
            {
                this.OnCommitted?.Invoke(s, e);
            };
            kafkaConsumer.OnCommitting += (s, e) =>
            {
                this.OnCommitting?.Invoke(s, e);
            };
            kafkaConsumer.OnRevoked += (s, e) =>
            {
                this.OnRevoked?.Invoke(s, e);
            };
            kafkaConsumer.OnRevoking += (s, e) =>
            {
                this.OnRevoking?.Invoke(s, e);
            };
            kafkaConsumer.OnErrorOccurred += (s, e) =>
            {
                this.OnErrorOccurred?.Invoke(s, e);
            };
        }

        private Task FilterNewPackage(KafkaMessage message)
        {
            if (this.OnMessageReceived == null || !this.filter(message)) return Task.CompletedTask;
            return this.OnMessageReceived(message);
        }

        public void Dispose()
        {
            this.kafkaConsumer.Dispose();
        }

        public Func<KafkaMessage, Task> OnMessageReceived { get; set; }
        public event EventHandler<Exception> OnErrorOccurred;
        public void Commit(ICollection<TopicPartitionOffset> partitionOffsets)
        {
            this.kafkaConsumer.Commit(partitionOffsets);
        }

        public void Commit()
        {
            this.kafkaConsumer.Commit();
        }

        public event EventHandler<CommittedEventArgs> OnCommitted;
        public event EventHandler<CommittingEventArgs> OnCommitting;
        public event EventHandler<RevokingEventArgs> OnRevoking;
        public event EventHandler<RevokedEventArgs> OnRevoked;
        public void Open()
        {
            this.kafkaConsumer.Open();
        }

        public void Close()
        {
            this.kafkaConsumer.Close();
        }
    }
}