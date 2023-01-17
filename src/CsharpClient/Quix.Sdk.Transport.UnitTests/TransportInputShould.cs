﻿using System;
using System.Threading.Tasks;
using FluentAssertions;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.UnitTests.Helpers;
using Xunit;

namespace Quix.Sdk.Transport.UnitTests
{
    public class TransportInputShould
    {
        [Fact]
        public void Send_ExceptionThrownByInput_ShouldThrowException()
        {
            // Arrange
            var exception = new Exception("I'm an exception");
            var passthrough = new Passthrough((p) => throw exception);
            var transportInput = new TransportInput(passthrough);

            var sentValue = TestModel.Create();
            var sentPackage = new Package<TestModel>(new Lazy<TestModel>(sentValue));

            Action action = () => transportInput.Send(sentPackage);

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
            var transportInput = new TransportInput(passthrough);

            var sentValue = TestModel.Create();
            var sentPackage = new Package<TestModel>(new Lazy<TestModel>(sentValue));

            // Act
            Action action = () => transportInput.Send(sentPackage).Wait(2000);

            // Assert
            action.Should().Throw<Exception>().WithMessage(exception.Message);
        }

    }
}
