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
            kafkaConsumer.MessageReceived = FilterNewPackage;
            kafkaConsumer.Committed += (s, e) =>
            {
                this.Committed?.Invoke(s, e);
            };
            kafkaConsumer.Committing += (s, e) =>
            {
                this.Committing?.Invoke(s, e);
            };
            kafkaConsumer.Revoked += (s, e) =>
            {
                this.Revoked?.Invoke(s, e);
            };
            kafkaConsumer.Revoking += (s, e) =>
            {
                this.Revoking?.Invoke(s, e);
            };
            kafkaConsumer.ErrorOccurred += (s, e) =>
            {
                this.ErrorOccurred?.Invoke(s, e);
            };
        }

        private Task FilterNewPackage(KafkaMessage message)
        {
            if (this.MessageReceived == null || !this.filter(message)) return Task.CompletedTask;
            return this.MessageReceived(message);
        }

        public void Dispose()
        {
            this.kafkaConsumer.Dispose();
        }

        public Func<KafkaMessage, Task> MessageReceived { get; set; }
        public event EventHandler<Exception> ErrorOccurred;
        public void Commit(ICollection<TopicPartitionOffset> partitionOffsets)
        {
            this.kafkaConsumer.Commit(partitionOffsets);
        }

        public void Commit()
        {
            this.kafkaConsumer.Commit();
        }

        public event EventHandler<CommittedEventArgs> Committed;
        public event EventHandler<CommittingEventArgs> Committing;
        public event EventHandler<RevokingEventArgs> Revoking;
        public event EventHandler<RevokedEventArgs> Revoked;
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