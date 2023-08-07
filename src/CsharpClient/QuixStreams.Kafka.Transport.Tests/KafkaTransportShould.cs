using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests
{
    public class KafkaTransportShould
    {
        [Fact]
        public async Task TransportConsumer_ShouldCorrespondToTransportProducer()
        {
            // This test checks that Transport Input and Output are reverse of each other
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = PackageSerializationMode.LegacyValue; 

            // Arrange
            var testBroker = new TestBroker();
            testBroker.MaxMessageSizeBytes = 18; // this tiny to force some splitting
            var transportProducer = new KafkaTransportProducer(testBroker, kafkaMessageSplitter: new KafkaMessageSplitter(testBroker.MaxMessageSizeBytes));
            var transportConsumer = new KafkaTransportConsumer(testBroker);

            TransportPackage packageReceived = null;
            transportConsumer.PackageReceived = (p) =>
            {
                packageReceived = p;
                return Task.CompletedTask;
            };

            var sentValue = TestModel.Create();
            var sentPackage = new TransportPackage<TestModel>("someKey", sentValue);

            // Act
            await transportProducer.Publish(sentPackage); // should be completed the moment packageReceived is set. Timeout is in case test fails;

            // Assert
            packageReceived.Should().NotBeNull();
            packageReceived.TryConvertTo<TestModel>(out var testPackageReceived).Should().BeTrue();
            testPackageReceived.Value.Equals(sentValue).Should().BeTrue();
        }
    }
}
