using System;
using System.Threading.Tasks;
using FluentAssertions;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.UnitTests.Helpers;
using Xunit;

namespace Quix.Streams.Transport.UnitTests
{
    public class TransportProducerShould
    {
        [Fact]
        public void Send_ExceptionThrownByInput_ShouldThrowException()
        {
            // Arrange
            var exception = new Exception("I'm an exception");
            var passthrough = new Passthrough((p) => throw exception);
            var transportProducer = new TransportProducer(passthrough);

            var sentValue = TestModel.Create();
            var sentPackage = new Package<TestModel>(new Lazy<TestModel>(sentValue));

            Action action = () => transportProducer.Publish(sentPackage);

            // Assert
            action.Should().Throw<Exception>().WithMessage(exception.Message);
        }

        [Fact]
        public void Send_TaskWithExceptionIsReturnedByInput_ShouldThrowException()
        {
            // Arrange
            var exception = new Exception("I'm an exception");
            var passthrough = new Passthrough((p) =>
            {
                var ts = new TaskCompletionSource<object>();
                ts.SetException(exception);
                return ts.Task;
            });
            var transportProducer = new TransportProducer(passthrough);

            var sentValue = TestModel.Create();
            var sentPackage = new Package<TestModel>(new Lazy<TestModel>(sentValue));

            // Act
            Action action = () => transportProducer.Publish(sentPackage).Wait(2000);

            // Assert
            action.Should().Throw<Exception>().WithMessage(exception.Message);
        }

    }
}
