using System;
using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests
{
    public class KafkaTransportProducerShould
    {
        [Fact]
        public void Publish_ExceptionThrownByInput_ShouldThrowException()
        {
            // Arrange
            var exception = new Exception("I'm an exception");
            var testBroker = new TestBroker((p) => throw exception);
            var transportProducer = new KafkaTransportProducer(testBroker);

            var sentValue = TestModel.Create();
            var sentPackage = new TransportPackage<TestModel>("someKey", sentValue);

            Func<Task> action = () => transportProducer.Publish(sentPackage);

            // Assert
            action.Should().Throw<Exception>().WithMessage(exception.Message);
        }

        [Fact]
        public void Send_TaskWithExceptionIsReturnedByInput_ShouldThrowException()
        {
            // Arrange
            var exception = new Exception("I'm an exception");
            var passthrough = new TestBroker((p) =>
            {
                var ts = new TaskCompletionSource<object>();
                ts.SetException(exception);
                return ts.Task;
            });
            var transportProducer = new KafkaTransportProducer(passthrough);

            var sentValue = TestModel.Create();
            var sentPackage = new TransportPackage<TestModel>("someKey", sentValue);

            // Act
            Action action = () => transportProducer.Publish(sentPackage).Wait(2000);

            // Assert
            action.Should().Throw<Exception>().WithMessage(exception.Message);
        }

    }
}
