using System;
using FluentAssertions;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Process.UnitTests.Helpers;
using Xunit;

namespace Quix.Sdk.Process.UnitTests
{
    public class StreamProcessShould
    {
        [Fact]
        public void Send_AfterAddComponent_ShouldExecuteSubscribersHandlers()
        {
            // Arrange
            IStreamProcess process = new StreamProcess();
            StreamComponent component = new StreamComponent();
            component.Input.LinkTo(component.Output);
            process.AddComponent(component);

            TestModel1 testModel1 = new TestModel1();
            TestModel1 handledModel1 = null;

            TestModel2 testModel2 = new TestModel2();
            TestModel2 handledModel2 = null;

            int num = 0;
            process.Subscribe<TestModel1>((stream, test) =>
            {
                handledModel1 = test;
                num++;
            });

            // Act
            process.Send(testModel1);
            process.Send(testModel2);

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
            Assert.Equal(1, num);
        }
        
        [Theory]
        [InlineData("valid-stream-id", false)]
        [InlineData("in/valid/stream/id", true)]
        [InlineData("in\\valid\\stream\\id", true)]
        public void Constructor_StreamId_ShouldDoExpected(string streamId, bool throwArgumentOutOfRangeException)
        {
            Action action = () =>  new StreamProcess(streamId);
            if (throwArgumentOutOfRangeException)
            {
                action.Should().Throw<ArgumentOutOfRangeException>();
            }
            else
            {
                action.Should().NotThrow<ArgumentOutOfRangeException>();
            }
        }


        [Fact]
        public void Send_WithPackageSubscription_ShouldExecutePackageSubscribersHandlers()
        {
            // Arrange
            IStreamProcess process = new StreamProcess();
            StreamComponent component = new StreamComponent();
            component.Input.LinkTo(component.Output);
            process.AddComponent(component);

            TestModel1 testModel1 = new TestModel1();
            TestModel1 handledModel1 = null;

            TestModel2 testModel2 = new TestModel2();

            StreamPackage handledPackage = null;

            process.Subscribe((stream, test) =>
            {
                handledPackage = test;
            });

            process.Subscribe<TestModel1>((stream, test) =>
            {
                handledModel1 = test;
            });

            // Act
            process.Send(new StreamPackage(typeof(TestModel1), testModel1));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Equal(testModel1, handledPackage.Value);

            // Act
            process.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Equal(testModel2, handledPackage.Value);
        }


    }

}


