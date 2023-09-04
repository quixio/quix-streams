using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using FluentAssertions;
using NSubstitute;
using Quix.TestBase.Extensions;
using QuixStreams;
using QuixStreams.Kafka.Transport;
using QuixStreams.Telemetry.Models;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Telemetry.UnitTests
{
    public class StreamPipelineFactoryShould
    {
        public StreamPipelineFactoryShould(ITestOutputHelper outputHelper)
        {
            QuixStreams.Logging.Factory = outputHelper.CreateLoggerFactory();
        }
        
        [Fact]
        public void StreamPackageReceived_NoPreviousStream_ShouldBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaTransportConsumer>();
            var factory = new TestStreamPipelineFactory(consumer, (s) => new StreamPipeline());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();

            // Act
            var package = new TransportPackage(typeof(object), "ABCDE", new object());
            consumer.OnPackageReceived(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
        }

#if DEBUG // too fragile on build server
        [Fact]
        public void StreamPackageReceived_NoPreviousStreamAndStreamCreateThrowsException_ShouldDoRetryLogic()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaTransportConsumer>();
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
            var package = new  TransportPackage(typeof(object), "somestreamid", new object());
            sw.Restart();

            Action action = () => consumer.OnPackageReceived(package).GetAwaiter().GetResult();

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
            var consumer = Substitute.For<IKafkaTransportConsumer>();
            var factory = new TestStreamPipelineFactory(consumer, (s) => new StreamPipeline());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new TransportPackage(typeof(object), "ABCDE", new object());
            consumer.OnPackageReceived(package);

            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);

            // Act
            package = new TransportPackage(typeof(object), "ABCDE", new object());
            consumer.OnPackageReceived(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
        }

        [Fact]
        public void StreamPipelineCloses_TrackedAsActiveStream_ShouldNoLongerBeTrackedAsActiveStream()
        {
            // Arrange
            var consumer = Substitute.For<IKafkaTransportConsumer>();
            var streamPipeline = new StreamPipeline();
            var factory = new TestStreamPipelineFactory(consumer, (s) => streamPipeline);
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new TransportPackage(typeof(object), streamPipeline.StreamId, new object());
            consumer.OnPackageReceived(package);

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
            var consumer = Substitute.For<IKafkaTransportConsumer>();
            var streamPipeline = new StreamPipeline();
            var factory = new TestStreamPipelineFactory(consumer, (s) => streamPipeline);
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();
            var package = new TransportPackage(typeof(object), streamPipeline.StreamId, new object());
            consumer.OnPackageReceived(package);

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
            var consumer = Substitute.For<IKafkaTransportConsumer>();
            var factory = new TestStreamPipelineFactory(consumer, (s) => new StreamPipeline());
            factory.ContextCache.GetAll().Keys.Count.Should().Be(0);
            factory.Open();

            // Act
            var package = new TransportPackage(typeof(object), null, new object());
            consumer.OnPackageReceived(package);

            // Assert
            factory.ContextCache.GetAll().Keys.Count.Should().Be(1);
            factory.ContextCache.GetAll().Keys.First().Should().Be(StreamPipeline.DefaultStreamIdWhenMissing);
        }

        class TestStreamPipelineFactory : StreamPipelineFactory
        {
            public const string TransportContextStreamIdKey = "StreamId";
            
            public TestStreamPipelineFactory(IKafkaTransportConsumer kafkaTransportConsumer, Func<string, IStreamPipeline> streamPipelineFactoryHandler) : this(kafkaTransportConsumer, streamPipelineFactoryHandler, new StreamContextCache())
            {
            }

            public TestStreamPipelineFactory(IKafkaTransportConsumer transportKafkaConsumer,
                Func<string, IStreamPipeline> streamPipelineFactoryHandler, IStreamContextCache cache) : base(
                transportKafkaConsumer, streamPipelineFactoryHandler, cache)
            {
                this.ContextCache = cache;
            }

            public readonly IStreamContextCache ContextCache;
        }

    }
}
