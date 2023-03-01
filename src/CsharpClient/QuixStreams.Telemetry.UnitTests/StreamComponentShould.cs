using System.Threading.Tasks;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.UnitTests.Helpers;
using Xunit;

namespace QuixStreams.Telemetry.UnitTests
{
    public class StreamComponentShould
    {
        [Fact]
        public void Send_WithSubscription_ShouldExecuteSubscribersHandlers()
        {
            // Arrange
            IOComponentConnection connectionPoint = new IOComponentConnection();

            TestModel1 testModel1 = new TestModel1();
            TestModel1 handledModel1 = null;

            TestModel2 testModel2 = new TestModel2();
            TestModel2 handledModel2 = null;

            connectionPoint.Subscribe<TestModel1>(test =>
            {
                handledModel1 = test;
            });

            // Act
            connectionPoint.Send(new StreamPackage(typeof(TestModel1), testModel1));
            connectionPoint.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
        }

        [Fact]
        public async Task Send_WithIntercept_ShouldExecuteInterceptHandlersInsteadOfSubscribers()
        {
            // Arrange
            IOComponentConnection connectionPoint = new IOComponentConnection();

            TestModel1 testModel1 = new TestModel1();
            TestModel1 handledModel1 = null;

            TestModel2 testModel2 = new TestModel2();
            TestModel2 handledModel2 = null;

            TestModel2 handledIntercept = null;

            connectionPoint.Subscribe<TestModel1>(test =>
            {
                handledModel1 = test;
            });

            connectionPoint.Subscribe<TestModel2>(test =>
            {
                handledModel2 = test;
            });

            connectionPoint.Intercept<TestModel2>(test =>
            {
                handledIntercept = test;
            });

            // Act
            await connectionPoint.Send(new StreamPackage(typeof(TestModel1), testModel1));
            await connectionPoint.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
            Assert.Equal(testModel2, handledIntercept);
        }


        [Fact]
        public void Send_WithPackageSubscription_ShouldExecutePackageSubscribersHandlers()
        {
            // Arrange
            IOComponentConnection connectionPoint = new IOComponentConnection();

            TestModel1 testModel1 = new TestModel1();
            TestModel1 handledModel1 = null;

            TestModel2 testModel2 = new TestModel2();

            StreamPackage handledPackage = null;

            connectionPoint.Subscribe(test =>
            {
                handledPackage = test;
            });

            connectionPoint.Subscribe<TestModel1>(test =>
            {
                handledModel1 = test;
            });

            // Act
            connectionPoint.Send(new StreamPackage(typeof(TestModel1), testModel1));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Equal(testModel1, handledPackage.Value);

            // Act
            connectionPoint.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Equal(testModel2, handledPackage.Value);
        }

        [Fact]
        public async Task StreamComponent_AfterLinkToAndIntercept_ShouldExecuteCorrectHandlers()
        {
            // Arrange
            IStreamComponent component = new StreamComponent();
            IIOComponentConnection input = component.Input;
            IIOComponentConnection output = component.Output;

            TestModel1 testModel1 = new TestModel1();
            TestModel1 handledModel1 = null;
            TestModel1 handledModel1Out = null;

            TestModel2 testModel2 = new TestModel2();
            TestModel2 handledModel2 = null;
            TestModel2 handledModel2Out = null;

            StreamPackage handledPackage = null;
            StreamPackage handledPackageOut = null;

            input.Subscribe(test =>
            {
                handledPackage = test;
            });

            input.Subscribe<TestModel1>(test =>
            {
                handledModel1 = test;
            });

            output.Subscribe(test =>
            {
                handledPackageOut = test;
            });

            output.Subscribe<TestModel1>(test =>
            {
                handledModel1Out = test;
            });

            // Act
            await input.Send(new StreamPackage(typeof(TestModel1), testModel1));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
            Assert.Equal(testModel1, handledPackage.Value);
            Assert.Null(handledModel1Out);
            Assert.Null(handledModel2Out);
            Assert.Null(handledPackageOut);

            // Act
            await input.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
            Assert.Equal(testModel2, handledPackage.Value);
            Assert.Null(handledModel1Out);
            Assert.Null(handledModel2Out);
            Assert.Null(handledPackageOut);

            // Act - LINK TO OUTPUT
            input.LinkTo(output);

            // Act
            await input.Send(new StreamPackage(typeof(TestModel1), testModel1));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
            Assert.Equal(testModel1, handledPackage.Value);
            Assert.Equal(testModel1, handledModel1Out);
            Assert.Null(handledModel2Out);
            Assert.Equal(testModel1, handledPackageOut.Value);

            // Act
            await input.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Null(handledModel2);
            Assert.Equal(testModel2, handledPackage.Value);
            Assert.Equal(testModel1, handledModel1Out);
            Assert.Null(handledModel2Out);
            Assert.Equal(testModel2, handledPackageOut.Value);

            // Act - INTERCEPT
            input.Intercept<TestModel2> (test =>
            {
                handledModel2 = test;
            });

            await input.Send(new StreamPackage(typeof(TestModel1), testModel1));
            await input.Send(new StreamPackage(typeof(TestModel2), testModel2));

            // Assert
            Assert.Equal(testModel1, handledModel1);
            Assert.Equal(testModel2, handledModel2);
            Assert.Equal(testModel1, handledPackage.Value);
            Assert.Equal(testModel1, handledModel1Out);
            Assert.Null(handledModel2Out);
            Assert.Equal(testModel1, handledPackageOut.Value);

        }

    }

}


