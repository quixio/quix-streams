using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Quix.TestBase.Extensions;
using QuixStreams.Telemetry.Common.Test;
using QuixStreams.Telemetry.Kafka;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.UnitTests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Telemetry.UnitTests
{
    public class KafkaConsumerProducerShould
    {
        public KafkaConsumerProducerShould(ITestOutputHelper helper)
        {
            QuixStreams.Logging.Factory = helper.CreateLoggerFactory();
        }
        
        [Fact]
        public async Task KafkaConsumerProducer_AfterSendSeveralStreams_ShouldReadProperStreams()
        {
            RegisterTestCodecs();

            // ARRANGE
            TestBroker testBroker = new TestBroker();
            var results = new List<(string, Type)>();
            var resultsModel1 = new ConcurrentBag<(string, string)>();
            var resultsModel2 = new ConcurrentBag<(string, string)>();

            TestModel1 testModel1 = new TestModel1() { Id = "model1" };
            TestModel2 testModel2 = new TestModel2() { Id = "model2" };

            bool streamStarted = false;

            // Create Kafka consumer
            var kafkaConsumer = new TestTelemetryKafkaConsumer(testBroker);
            kafkaConsumer.ForEach(streamId =>
            {
                streamStarted = true;

                var s = new StreamPipeline(streamId);
                s.Subscribe((streamPipeline, package) =>
                {
                    results.Add((streamPipeline.StreamId, package.Type));
                });
                s.Subscribe<TestModel1>((streamPipeline, model) =>
                {
                    resultsModel1.Add((streamPipeline.StreamId, model.Id));
                });
                s.Subscribe<TestModel2>((streamPipeline, model) =>
                {
                    resultsModel2.Add((streamPipeline.StreamId, model.Id));
                });
                return s;
            });

            kafkaConsumer.Start();

            // Create streams
            var stream1 = new StreamPipeline()
                .AddComponent(new TestTelemetryKafkaProducer(testBroker, "StreamId_1"));
            var stream2 = new StreamPipeline()
                .AddComponent(new TestTelemetryKafkaProducer(testBroker, "StreamId_2"));
            var stream3 = new StreamPipeline()
                .AddComponent(new TestTelemetryKafkaProducer(testBroker, "StreamId_3"));

            // ACT
            await stream1.Send(testModel1);
            await stream2.Send(testModel1);
            await stream2.Send(testModel2);
            await stream3.Send(testModel2);

            // ASSERT
            Assert.Equal(3, kafkaConsumer.ContextCache.GetAll().Count);
            Assert.Contains(("StreamId_1", typeof(TestModel1)), results);
            Assert.DoesNotContain(("StreamId_1", typeof(TestModel2)), results);
            Assert.Contains(("StreamId_2", typeof(TestModel1)), results);
            Assert.Contains(("StreamId_2", typeof(TestModel2)), results);
            Assert.DoesNotContain(("StreamId_3", typeof(TestModel1)), results);
            Assert.Contains(("StreamId_3", typeof(TestModel2)), results);

            
            // ASSERT MODEL SUBSCRIPTION
            Assert.Equal(2, resultsModel1.Count);
            Assert.Equal(2, resultsModel2.Count);
            Assert.Contains(("StreamId_1", "model1"), resultsModel1);
            Assert.Contains(("StreamId_2", "model1"), resultsModel1);
            Assert.Contains(("StreamId_2", "model2"), resultsModel2);
            Assert.Contains(("StreamId_3", "model2"), resultsModel2);

            // ACT
            var streamEnd = new StreamEnd();
            await stream1.Send(streamEnd);

            // ASSERT
            Assert.Equal(2, kafkaConsumer.ContextCache.GetAll().Count);

            // ACT - RE-OPEN TEST
            streamStarted = false;
            await stream1.Send(testModel1);

            // ASSERT RE-OPEN TEST
            Assert.True(streamStarted);
            Assert.Equal(3, kafkaConsumer.ContextCache.GetAll().Count);
        }

        [Fact]
        public void KafkaConsumerProducer_WithoutRegisterCodecs_ShouldRaiseExceptions()
        {
            UnregisterTestCodecs();

            // ARRANGE
            TestBroker testBroker = new TestBroker();
            TestModel1 testModel1 = new TestModel1();
            bool raised = false;

            // Create Kafka consumer
            TelemetryKafkaConsumer telemetryKafkaConsumer = new TestTelemetryKafkaConsumer(testBroker);
            telemetryKafkaConsumer.ForEach(streamId =>
            {
                var s = new StreamPipeline(streamId);
                return s;
            });

            // Create Kafka producer
            TelemetryKafkaProducer telemetryKafkaProducer = new TestTelemetryKafkaProducer(testBroker, "StreamId_1");
            var stream1 = new StreamPipeline()
                .AddComponent(telemetryKafkaProducer);

            telemetryKafkaProducer.OnWriteException += (sender, e) => raised = true;

            // ACT
            stream1.Send(testModel1);

            // ASSERT
            Assert.True(raised);
        }

        [Fact]
        public void KafkaConsumerProducer_WithErrorsOnSend_ShouldRaiseExceptions()
        {
            RegisterTestCodecs();

            // ARRANGE
            TestBroker testBroker = new TestBroker(true);
            TestModel1 testModel1 = new TestModel1();
            bool raised = false;

            // Create Kafka consumer
            TelemetryKafkaConsumer telemetryKafkaConsumer = new TestTelemetryKafkaConsumer(testBroker);
            telemetryKafkaConsumer.ForEach(streamId =>
            {
                var s = new StreamPipeline(streamId);
                return s;
            });

            // Create Kafka producer
            TelemetryKafkaProducer telemetryKafkaProducer = new TestTelemetryKafkaProducer(testBroker, "StreamId_1");
            var stream1 = new StreamPipeline()
                .AddComponent(telemetryKafkaProducer);

            telemetryKafkaProducer.OnWriteException += (sender, e) => raised = true;

            // ACT
            stream1.Send(testModel1);

            // ASSERT
            Assert.True(raised);
        }

        private static void RegisterTestCodecs()
        {
            QuixStreams.Transport.Registry.CodecRegistry.RegisterCodec(typeof(StreamEnd).Name, new QuixStreams.Transport.Fw.Codecs.DefaultJsonCodec<StreamEnd>());
            QuixStreams.Transport.Registry.CodecRegistry.RegisterCodec(new QuixStreams.Transport.Fw.ModelKey(typeof(TestModel1)), new QuixStreams.Transport.Fw.Codecs.DefaultJsonCodec<TestModel1>());
            QuixStreams.Transport.Registry.CodecRegistry.RegisterCodec(new QuixStreams.Transport.Fw.ModelKey(typeof(TestModel2)), new QuixStreams.Transport.Fw.Codecs.DefaultJsonCodec<TestModel2>());
        }

        private static void UnregisterTestCodecs()
        {
            QuixStreams.Transport.Registry.CodecRegistry.ClearCodecs(typeof(StreamEnd).Name);
            QuixStreams.Transport.Registry.CodecRegistry.ClearCodecs(new QuixStreams.Transport.Fw.ModelKey(typeof(TestModel1)));
            QuixStreams.Transport.Registry.CodecRegistry.ClearCodecs(new QuixStreams.Transport.Fw.ModelKey(typeof(TestModel2)));
        }

    }

}


