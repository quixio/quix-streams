using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests
{
    public class KafkaTransportConsumerShould
    {
        [Fact]
        public void Commit_WithTransport_ShouldAckConsumer()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaConsumer>();
            ICollection<TopicPartitionOffset> committed = null;
            consumer.Commit(Arg.Do<ICollection<TopicPartitionOffset>>(x => committed = x));
            var transportConsumer = new KafkaTransportConsumer(consumer);
            
            var transportContext = new TopicPartitionOffset ( new TopicPartition("sometopic", new Partition(1)), new Offset(123));
            var transportContext2 = new TopicPartitionOffset ( new TopicPartition("sometopic", new Partition(0)), new Offset(333));
            
            // Act
            transportConsumer.Commit(new [] {transportContext, transportContext2});

            // Assert
            committed.Should().BeEquivalentTo(transportContext, transportContext2);
        }
        
        [Fact]
        public async Task Setup_ConsumerRaisesEnoughForAutoCommitToKickIn_ShouldCommitIntoConsumer()
        {
            // Arrange
            var broker = new TestBroker();
            CommittedEventArgs committed = null;
            broker.OnCommitted += (sender, args) => committed = args;
            var transportConsumer = new KafkaTransportConsumer(broker, o=> o.CommitOptions.CommitEvery = 1);
            
            var message = ModelFactory.CreateKafkaMessage("SomeKey", new { Some = "Value" });
            
            // Act
            await broker.Publish(message);

            // Assert
            committed.Should().NotBeNull();
        }
        
        [Fact]
        public void Setup_ConsumerRaisesRevokingEvent_ShouldAlsoRaiseRevokingEvent()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaConsumer>();
            var transportConsumer = new KafkaTransportConsumer(consumer);
            var revokingInvokes = new List<object>();
            transportConsumer.OnRevoking += (sender, args) =>
            {
                revokingInvokes.Add(args);
            };

            var revokingOffset = new TopicPartitionOffset("SomeTopic", new Partition(0), new Offset(123));
            
            // Act
            var expectedArgs = new RevokingEventArgs(new List<TopicPartitionOffset>() {revokingOffset});
            consumer.OnRevoking += Raise.EventWith(expectedArgs);

            // Assert
            revokingInvokes.Should().BeEquivalentTo(new[] {expectedArgs});
        }
        
        
        [Fact]
        public void Setup_ConsumerRaisesRevokedEvent_ShouldAlsoRaiseRevokedEvent()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaConsumer>();
            var transportConsumer = new KafkaTransportConsumer(consumer);
            var revokedInvokes = new List<object>();
            transportConsumer.OnRevoked += (sender, args) =>
            {
                revokedInvokes.Add(args);
            };
            
            var revokedOffset = new TopicPartitionOffset("SomeTopic", new Partition(0), new Offset(123));

            
            // Act
            var expectedArgs = new RevokedEventArgs(new List<TopicPartitionOffset>() { revokedOffset});
            consumer.OnRevoked += Raise.EventWith(expectedArgs);

            // Assert
            revokedInvokes.Should().BeEquivalentTo(new[] {expectedArgs});
        }

        [Fact]
        public void Setup_ConsumerRaisesCommitted_ShouldAlsoRaiseCommitted()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaConsumer>();
            CommittedEventArgs committed = null;
            var transportConsumer = new KafkaTransportConsumer(consumer, o=> o.CommitOptions.CommitEvery = 1);
            transportConsumer.OnCommitted += (sender, args) => committed = args;
            
            var committedOffset = new TopicPartitionOffset("SomeTopic", new Partition(0), new Offset(123));

            
            // Act
            var expectedArgs = new CommittedEventArgs(new CommittedOffsets(new List<TopicPartitionOffsetError>() {new TopicPartitionOffsetError(committedOffset, new Error(ErrorCode.NoError))},new Error(ErrorCode.NoError)));
            consumer.OnCommitted += Raise.EventWith(expectedArgs);

            // Assert
            committed.Should().BeEquivalentTo(expectedArgs);
        }
        
                
        [Fact]
        public void Setup_ConsumerRaisesCommitting_ShouldAlsoRaiseCommitting()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaConsumer>();
            CommittingEventArgs committingEventArgs = null;
            var transportConsumer = new KafkaTransportConsumer(consumer, o=> o.CommitOptions.CommitEvery = 1);
            transportConsumer.OnCommitting += (sender, args) => committingEventArgs =args;
            
            var committingOffset = new TopicPartitionOffset("SomeTopic", new Partition(0), new Offset(123));

            
            // Act
            var expectedArgs = new CommittingEventArgs(new List<TopicPartitionOffset>() { committingOffset});
            consumer.OnCommitting += Raise.EventWith(expectedArgs);

            // Assert
            committingEventArgs.Should().BeEquivalentTo(expectedArgs);
        }
    }
}
