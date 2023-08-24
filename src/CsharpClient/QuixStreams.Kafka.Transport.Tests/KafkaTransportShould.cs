using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests
{
    public class KafkaTransportShould
    {
        [Theory]
        [MemberData(nameof(TestData))]
        public async Task TransportConsumer_ShouldCorrespondToTransportProducer(PackageSerializationMode mode, bool enableMessageSplit, int maxMessageSize, int expectedMessageCount)
        {
            // This test checks that Transport Input and Output are reverse of each other
            PackageSerializationSettings.Mode = mode;
            PackageSerializationSettings.EnableMessageSplit = enableMessageSplit; 

            // Arrange
            var testBroker = new TestBroker();
            testBroker.MaxMessageSizeBytes = maxMessageSize; // this tiny to force some splitting
            var transportProducer = new KafkaTransportProducer(testBroker);
            var transportConsumer = new KafkaTransportConsumer(testBroker);

            TransportPackage packageReceived = null;
            transportConsumer.OnPackageReceived = (p) =>
            {
                packageReceived = p;
                return Task.CompletedTask;
            };

            var sentValue = TestModel.Create(500);
            var sentPackage = new TransportPackage<TestModel>("someKey", sentValue);

            // Act
            await transportProducer.Publish(sentPackage); // should be completed the moment packageReceived is set. Timeout is in case test fails;

            // Assert
            packageReceived.Should().NotBeNull();
            packageReceived.TryConvertTo<TestModel>(out var testPackageReceived).Should().BeTrue();
            testPackageReceived.Value.Equals(sentValue).Should().BeTrue();
            testBroker.MessageCount.Should().Be(expectedMessageCount);
        }
        
        public static IEnumerable<object[]> TestData()
        {
            yield return new object[] { PackageSerializationMode.LegacyValue, true, 500, 4};
            yield return new object[] { PackageSerializationMode.LegacyValue, false, 18, 1 };
            yield return new object[] { PackageSerializationMode.Header, true, 500, 19 };
            yield return new object[] { PackageSerializationMode.Header, false, 400, 1};
        }
    }
}
