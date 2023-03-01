using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FluentAssertions;
using NSubstitute;
using QuixStreams;
using QuixStreams.Telemetry.Models;
using QuixStreams.Transport.IO;
using Quix.TestBase.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Telemetry.UnitTests
{
    public class StreamProcessFactoryShould
    {
        public StreamProcessFactoryShould(ITestOutputHelper outputHelper)
        {
            Logging.Factory = outputHelper.CreateLoggerFactory();
        }
        
        [Fact]
        public void StreamPackageReceived_NoPreviousStream_ShouldBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var factory = new TestStreamProcessFactory(consumer, (s) => new StreamProcess());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();

            // Act
            var package = new Package(typeof(object), new Lazy<object>(() => new object()), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamProcessFactory.TransportContextStreamIdKey, "ABCDE"}
            }));
            consumer.OnNewPackage(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
        }

#if DEBUG // too fragile on build server
        [Fact]
        public void StreamPackageReceived_NoPreviousStreamAndStreamCreateThrowsException_ShouldDoRetryLogic()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var elapsedTimes = new List<long>();
            var sw = new Stopwatch();
            var factory = new TestStreamProcessFactory(consumer, (s) =>
            {
                elapsedTimes.Add(sw.ElapsedMilliseconds);
                sw.Restart();
                throw new Exception("I am a baaad exception");
            });
            factory.RetryIncrease = 50;
            factory.MaxRetryDuration = 200;
            factory.MaxRetries = 10;
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new Lazy<object>(() => new object()), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamProcessFactory.TransportContextStreamIdKey, "somestreamid"}
            }));
            sw.Restart();

            Action action = () => consumer.OnNewPackage(package).GetAwaiter().GetResult();

            // Act & Assert

            action.Should().Throw<Exception>().Which.Message.Should()
                .BeEquivalentTo("Exception while creating a new stream process for stream somestreamid. Failed 10 times. Reached maximum retry count.");
            
            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            for (int i = 1; i < elapsedTimes.Count; i++) // first is ignored, because that won't have any wait on it
            {
                var waited = elapsedTimes[i];
                var expectedWait = Math.Min(i * factory.RetryIncrease, factory.MaxRetryDuration);
                const int range = 20;
                waited.Should().BeInRange(expectedWait - range, expectedWait + range, $"the iteration {i} should wait for about this long");
            }

            elapsedTimes.Count.Should().Be(10); // first ignored + 9 attempts. 10th attempt raises exception
        }
#endif        

        [Fact]
        public void StreamPackageReceived_PrevioslyActiveStream_ShouldNotAddAnotherTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var factory = new TestStreamProcessFactory(consumer, (s) => new StreamProcess());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new Lazy<object>(() => new object()), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamProcessFactory.TransportContextStreamIdKey, "ABCDE"}
            }));
            consumer.OnNewPackage(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            package = new Package(typeof(object), new Lazy<object>(() => new object()), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamProcessFactory.TransportContextStreamIdKey, "ABCDE"}
            }));
            consumer.OnNewPackage(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
        }

        [Fact]
        public void StreamProcessCloses_TrackedAsActiveStream_ShouldNoLongerBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var process = new StreamProcess();
            var factory = new TestStreamProcessFactory(consumer, (s) => process);
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new Lazy<object>(() => new object()), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamProcessFactory.TransportContextStreamIdKey, process.StreamId}
            }));
            consumer.OnNewPackage(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            factory.ContextCache.GetAll().Values.First().StreamProcess.Close();

            // Assert

            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
        }

        [Fact]
        public void StreamEndReceived_TrackedAsActiveStream_ShouldNoLongerBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var process = new StreamProcess();
            var factory = new TestStreamProcessFactory(consumer, (s) => process);
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new Lazy<object>(() => new object()), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamProcessFactory.TransportContextStreamIdKey, process.StreamId}
            }));
            consumer.OnNewPackage(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            process.Send(new StreamEnd());

            // Assert

            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
        }


        class TestStreamProcessFactory : StreamProcessFactory
        {
            public const string TransportContextStreamIdKey = "StreamId";
            
            public TestStreamProcessFactory(Transport.IO.IConsumer transportConsumer, Func<string, IStreamProcess> streamProcessFactoryHandler) : this(transportConsumer, streamProcessFactoryHandler, new StreamContextCache())
            {
            }

            public TestStreamProcessFactory(Transport.IO.IConsumer transportConsumer,
                Func<string, IStreamProcess> streamProcessFactoryHandler, IStreamContextCache cache) : base(
                transportConsumer, streamProcessFactoryHandler, cache)
            {
                this.ContextCache = cache;
            }

            public readonly IStreamContextCache ContextCache;

            protected override bool TryGetStreamId(TransportContext transportContext, out string streamId)
            {
                streamId = transportContext[TransportContextStreamIdKey].ToString();
                return true;
            }
        }

    }
}
