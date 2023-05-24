using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FluentAssertions;
using NSubstitute;
using Quix.TestBase.Extensions;
using QuixStreams.Telemetry.Models;
using QuixStreams.Transport.IO;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Telemetry.UnitTests
{
    public class StreamPipelineFactoryShould
    {
        public StreamPipelineFactoryShould(ITestOutputHelper outputHelper)
        {
            Logging.Factory = outputHelper.CreateLoggerFactory();
        }
        
        [Fact]
        public void StreamPackageReceived_NoPreviousStream_ShouldBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var factory = new TestStreamPipelineFactory(consumer, (s) => new StreamPipeline());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();

            // Act
            var package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamPipelineFactory.TransportContextStreamIdKey, "ABCDE"}
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
            var factory = new TestStreamPipelineFactory(consumer, (s) =>
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
            var package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamPipelineFactory.TransportContextStreamIdKey, "somestreamid"}
            }));
            sw.Restart();

            Action action = () => consumer.OnNewPackage(package).GetAwaiter().GetResult();

            // Act & Assert

            action.Should().Throw<Exception>().Which.Message.Should()
                .BeEquivalentTo("Exception while creating a new stream pipeline for stream somestreamid. Failed 10 times. Reached maximum retry count.");
            
            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            for (int i = 1; i < elapsedTimes.Count; i++) // first is ignored, because that won't have any wait on it
            {
                var waited = elapsedTimes[i];
                var expectedWait = Math.Min(i * factory.RetryIncrease, factory.MaxRetryDuration);
                const int range = 35;
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
            var factory = new TestStreamPipelineFactory(consumer, (s) => new StreamPipeline());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamPipelineFactory.TransportContextStreamIdKey, "ABCDE"}
            }));
            consumer.OnNewPackage(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamPipelineFactory.TransportContextStreamIdKey, "ABCDE"}
            }));
            consumer.OnNewPackage(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
        }

        [Fact]
        public void StreamPipelineCloses_TrackedAsActiveStream_ShouldNoLongerBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var streamPipeline = new StreamPipeline();
            var factory = new TestStreamPipelineFactory(consumer, (s) => streamPipeline);
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamPipelineFactory.TransportContextStreamIdKey, streamPipeline.StreamId}
            }));
            consumer.OnNewPackage(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            factory.ContextCache.GetAll().Values.First().StreamPipeline.Close();

            // Assert

            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
        }

        [Fact]
        public void StreamEndReceived_TrackedAsActiveStream_ShouldNoLongerBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var streamPipeline = new StreamPipeline();
            var factory = new TestStreamPipelineFactory(consumer, (s) => streamPipeline);
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                {TestStreamPipelineFactory.TransportContextStreamIdKey, streamPipeline.StreamId}
            }));
            consumer.OnNewPackage(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            streamPipeline.Send(new StreamEnd());

            // Assert

            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
        }

        [Fact]
        public void StreamPackageReceived_WithoutStreamId_ShouldUseDefaultStreamId()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer>();
            var factory = new TestStreamPipelineFactory(consumer, (s) => new StreamPipeline());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();

            // Act
            var package = new Package(typeof(object), new object(), null, new TransportContext(new Dictionary<string, object>
            {
                // empty transport context (no stream id)   
            }));
            consumer.OnNewPackage(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
            factory.ContextCache.GetAll().Keys.First().Should().Be(StreamPipeline.DefaultStreamIdWhenMissing);
        }

        class TestStreamPipelineFactory : StreamPipelineFactory
        {
            public const string TransportContextStreamIdKey = "StreamId";
            
            public TestStreamPipelineFactory(Transport.IO.IConsumer transportConsumer, Func<string, IStreamPipeline> streamPipelineFactoryHandler) : this(transportConsumer, streamPipelineFactoryHandler, new StreamContextCache())
            {
            }

            public TestStreamPipelineFactory(Transport.IO.IConsumer transportConsumer,
                Func<string, IStreamPipeline> streamPipelineFactoryHandler, IStreamContextCache cache) : base(
                transportConsumer, streamPipelineFactoryHandler, cache)
            {
                this.ContextCache = cache;
            }

            public readonly IStreamContextCache ContextCache;

            protected override bool TryGetStreamId(TransportContext transportContext, out string streamId)
            {
                return transportContext.TryGetTypedValue(TransportContextStreamIdKey, out streamId);
            }
        }

    }
}
