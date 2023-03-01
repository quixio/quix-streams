using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.UnitTests.Helpers;
using Xunit;

namespace QuixStreams.Transport.UnitTests
{
    public class TransportShould
    {
        [Fact]
        public void TransportConsumer_ShouldCorrespondToTransportProducer()
        {
            // This test checks that Transport Input and Output are reverse of each other

            // Arrange
            var passthrough = new Passthrough();
            var byteSplitter = new ByteSplitter(15); // this tiny to force some splitting
            var transportProducer = new TransportProducer(passthrough, byteSplitter);
            var transportConsumer = new TransportConsumer(passthrough);

            Package packageReceived = null;
            transportConsumer.OnNewPackage = (p) =>
            {
                packageReceived = p;
                return Task.CompletedTask;
            };

            var sentMetaData = new MetaData(new Dictionary<string, string>() {{"TestKey", "TestValue"}});
            var sentValue = TestModel.Create();
            var sentPackage = new Package<TestModel>(new Lazy<TestModel>(sentValue), sentMetaData);

            // Act
            transportProducer.Publish(sentPackage).Wait(2000); // should be completed the moment packageReceived is set. Timeout is in case test fails;


            // Assert
            packageReceived.Should().NotBeNull();
            packageReceived.TryConvertTo<TestModel>(out var testPackageReceived).Should().BeTrue();
            testPackageReceived.Value.Value.Equals(sentValue).Should().BeTrue();
            testPackageReceived.MetaData.Should().BeEquivalentTo(sentMetaData);
        }
    }
}
