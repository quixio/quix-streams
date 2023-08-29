using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Kafka.Transport.SerDes;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes
{
    public class KafkaMessageMergerShould
    {
        [Fact]
        public void Modify_MergeReturnsBytes_ShouldRaisePackageAndReturnCompletedTask()
        {
            // Arrange
            var random = new Random();
            var bytes = new byte[500];
            random.NextBytes(bytes);
            var package = new KafkaMessage(null, bytes, null);
            var merger = new KafkaMessageMerger(new KafkaMessageBuffer());

            KafkaMessage kafkaMessage = null; 
            merger.OnMessageAvailable = (message) =>
            {
                kafkaMessage = message;
                return Task.CompletedTask;
            };

            // Act
            var task = merger.Merge(package);

            // Assert
            task.Wait(2000);
            task.IsCompleted.Should().BeTrue();
            kafkaMessage.Should().NotBeNull();
            (kafkaMessage.Value).Should().BeEquivalentTo(package.Value);
        }

        [Theory]
        [InlineData(PackageSerializationMode.Header)]
        [InlineData(PackageSerializationMode.LegacyValue)]
        public async Task Modify_SplitPackageInterweavedWithOtherPackages_ShouldRaiseInExpectedOrder(PackageSerializationMode mode)
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Message1_segment1] [Message2_segment1] [Message3] [Message1_segment2] [Message2_segment2]
            // then the outcome is [Message1_merged] [Message2_merged] [Message3]
            
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMerger(new KafkaMessageBuffer());
            
            var message1Segments = this.CreateSplitMessage(2, out var message1);
            var message2Segments = this.CreateSplitMessage(2, out var message2);
            var message3 = this.CreateMessage();
            
            var expectedOrder = new List<byte[]>()
            {
                message1.Value,
                message2.Value,
                message3.Value
            };

            var packagesReceived = new List<byte[]>();
            merger.OnMessageAvailable = (message) =>
            {
                packagesReceived.Add(message.Value);
                return Task.CompletedTask;
            };

            // Act
            await merger.Merge(message1Segments[0]);
            packagesReceived.Count.Should().Be(0); // only segments so far
            await merger.Merge(message2Segments[0]);
            packagesReceived.Count.Should().Be(0); // only segments so far
            await merger.Merge(message3);
            packagesReceived.Count.Should().Be(0); // have a normal package, but in-between segments
            await merger.Merge(message1Segments[1]);
            packagesReceived.Count.Should().Be(1); // first package segments arrived, but the normal package is still between segments
            await merger.Merge(message2Segments[1]);
            packagesReceived.Count.Should().Be(3); // the last segment arrived for the package wrapping the normal package, so both the normal and the merged release

            // Assert
            packagesReceived.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
        }
        

        [Theory]
        [InlineData(PackageSerializationMode.Header)]
        [InlineData(PackageSerializationMode.LegacyValue)]
        public async Task Modify_SplitPackageInterweavedWithOtherAndSplitPackageExpires_ShouldRaiseInExpectedOrder(PackageSerializationMode mode)
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Message1_segment1] [Message2] [Message1_segment2] [Message3] but message1 expires before message1_segment2 arrived 
            // then the outcome is [Message2], [Message3], as segment 2 should be discarded
            
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = mode;
            var buffer = new KafkaMessageBuffer();
            var merger = new KafkaMessageMerger(buffer);
            var message1Segments = this.CreateSplitMessage(2, out var message1);
            var message2 = this.CreateMessage();
            var message3 = this.CreateMessage();
            
            var expectedOrder = new List<byte[]>()
            {
                message2.Value,
                message3.Value
            };

            var packagesReceived = new List<byte[]>();
            merger.OnMessageAvailable = (message) =>
            {
                packagesReceived.Add(message.Value);
                return Task.CompletedTask;
            };

            // Act
            await merger.Merge(message1Segments[0]);
            var message1BufferId = buffer.GetBufferIds().First();
            packagesReceived.Count.Should().Be(0);
            await merger.Merge(message2);
            packagesReceived.Count.Should().Be(0); // blocked by segment 1
            buffer.Purge(message1BufferId); // IMPORTANT! this is before p2
            packagesReceived.Count.Should().Be(1);
            await merger.Merge(message1Segments[1]);
            packagesReceived.Count.Should().Be(1);
            await merger.Merge(message3);
            packagesReceived.Count.Should().Be(2); // because previous segment shouldn't block

            // Assert
            packagesReceived.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
        }
        
        [Theory]
        [InlineData(PackageSerializationMode.Header)]
        [InlineData(PackageSerializationMode.LegacyValue)]
        public void Modify_MergeReturnsNull_ShouldNotRaisePackageAndReturnCompletedTask(PackageSerializationMode mode)
        {
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = mode;
            var buffer = new KafkaMessageBuffer();
            var merger = new KafkaMessageMerger(buffer);
            
            var message1Segments = this.CreateSplitMessage(2, out var message1);

            KafkaMessage receivedMessage = null;
            merger.OnMessageAvailable = (message) =>
            {
                receivedMessage = message;
                return Task.CompletedTask;
            };

            // Act
            var task = merger.Merge(message1Segments[0]);

            // Assert
            task.Should().Be(Task.CompletedTask);
        }
        
        [Theory]
        [InlineData(PackageSerializationMode.Header)]
        [InlineData(PackageSerializationMode.LegacyValue)]
        public async Task Modify_SplitPackageInterweavedWithOtherAndSplitPackageRevoked_ShouldDiscardRevokedAndRaiseInExpectedOrder(PackageSerializationMode mode)
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Message1_segment1] [Message2] [Message1_segment2] [Message3] but source gets revoked and causes message 1 to
            // disappear then should raise [Message2] and [Message3]
            
            // Arrange
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = mode;
            var buffer = new KafkaMessageBuffer();
            var merger = new KafkaMessageMerger(buffer);
            var message1Segments = this.CreateSplitMessage(3, out var message1);
            var message2 = this.CreateMessage();
            var message3 = this.CreateMessage();
            
            var expectedOrder = new List<byte[]>()
            {
                message2.Value,
                message3.Value
            };

            var packagesReceived = new List<byte[]>();
            merger.OnMessageAvailable = (message) =>
            {
                packagesReceived.Add(message.Value);
                return Task.CompletedTask;
            };

            // Act
            await merger.Merge(message1Segments[0]);
            var message1BufferId = buffer.GetBufferIds().First();
            packagesReceived.Count.Should().Be(0);
            await merger.Merge(message2);
            packagesReceived.Count.Should().Be(0); // blocked by segment 1
            packagesReceived.Count.Should().Be(0);
            await merger.Merge(message1Segments[1]);
            packagesReceived.Count.Should().Be(0);
            await merger.Merge(message3);
            packagesReceived.Count.Should().Be(0); // because previous segment shouldn't block
            buffer.Purge(message1BufferId);
            packagesReceived.Count.Should().Be(2); // because previous segment shouldn't block
            
            // Assert
            packagesReceived.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
        }
        
        
        
        private KafkaMessage CreateMessage(int messageSize = 10)
        {
            var random = new Random();

            var messageBytes = new byte[messageSize];
            random.NextBytes(messageBytes);

            return new KafkaMessage(null, messageBytes, null);
        }
        
        private List<KafkaMessage> CreateSplitMessage(int segmentCount, out KafkaMessage originalMessage)
        {
            var expectedOverhead = PackageSerializationSettings.Mode switch
            {
                PackageSerializationMode.Header => KafkaMessageSplitter.ExpectedHeaderSplitInfoSize,
                PackageSerializationMode.LegacyValue => Constants.LegacyMessageSeparatorInfoLength,
                _ => 0
            };

            var valuableMessageSize = 500;
            var allowedKafkaMessage = expectedOverhead + valuableMessageSize;
            var splitter = new KafkaMessageSplitter(allowedKafkaMessage);
            

            // The original message must be bigger than the overhead, else there won't be a split
            var originalMessageSize = segmentCount * valuableMessageSize;
            
            originalMessage = this.CreateMessage(originalMessageSize);
            
            var segments = splitter.Split(originalMessage).ToList();
            return segments;
        }
    }
}