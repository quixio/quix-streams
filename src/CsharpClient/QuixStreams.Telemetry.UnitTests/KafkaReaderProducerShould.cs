using System;
using System.Collections.Generic;
using Quix.TestBase.Extensions;
using QuixStreams;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Kafka.Transport.Tests.Helpers;
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
        public void KafkaConsumerProducer_AfterSendSeveralStreams_ShouldReadProperStreams()
        {
            RegisterTestCodecs();

            // ARRANGE
            TestBroker testBroker = new TestBroker();
            var results = new List<(string, Type)>();
            var resultsModel1 = new List<(string, string)>();
            var resultsModel2 = new List<(string, string)>();

            TestModel1 testModel1 = new TestModel1() { Id = "model1" };
            TestModel2 testModel2 = new TestModel2() { Id = "model2" };

            bool streamStarted = false;

            // Create Kafka consumer
            var telemetryKafkaConsumer = new TelemetryKafkaConsumer(testBroker, null);
            telemetryKafkaConsumer.ForEach(streamId =>
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

            telemetryKafkaConsumer.Start();

            // Create streams
            var stream1 = new StreamPipeline()
                .AddComponent(new TelemetryKafkaProducer(testBroker, "StreamId_1"));
            var stream2 = new StreamPipeline()
                .AddComponent(new TelemetryKafkaProducer(testBroker, "StreamId_2"));
            var stream3 = new StreamPipeline()
                .AddComponent(new TelemetryKafkaProducer(testBroker, "StreamId_3"));

            // ACT
            stream1.Send(testModel1);
            stream2.Send(testModel1);
            stream2.Send(testModel2);
            stream3.Send(testModel2);

            // ASSERT
            Assert.Equal(3, telemetryKafkaConsumer.ContextCache.GetAll().Count);
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
            stream1.Send(streamEnd);

            // ASSERT
            Assert.Equal(2, telemetryKafkaConsumer.ContextCache.GetAll().Count);

            // ACT - RE-OPEN TEST
            streamStarted = false;
            stream1.Send(testModel1);

            // ASSERT RE-OPEN TEST
            Assert.True(streamStarted);
            Assert.Equal(3, telemetryKafkaConsumer.ContextCache.GetAll().Count);
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
            TelemetryKafkaConsumer telemetryKafkaConsumer = new TelemetryKafkaConsumer(testBroker, null);
            telemetryKafkaConsumer.ForEach(streamId =>
            {
                var s = new StreamPipeline(streamId);
                return s;
            });

            // Create Kafka producer
            var telemetryKafkaProducer = new TelemetryKafkaProducer(testBroker);
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
            TestBroker testBroker = new TestBroker((m) => throw new Exception());
            TestModel1 testModel1 = new TestModel1();
            bool raised = false;

            // Create Kafka consumer
            TelemetryKafkaConsumer telemetryKafkaConsumer = new TelemetryKafkaConsumer(testBroker, null);
            telemetryKafkaConsumer.ForEach(streamId =>
            {
                var s = new StreamPipeline(streamId);
                return s;
            });

            // Create Kafka producer
            TelemetryKafkaProducer telemetryKafkaProducer = new TelemetryKafkaProducer(testBroker, "StreamId_1");
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
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(typeof(StreamEnd).Name, new DefaultJsonCodec<StreamEnd>());
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(new ModelKey(typeof(TestModel1)), new DefaultJsonCodec<TestModel1>());
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(new ModelKey(typeof(TestModel2)), new DefaultJsonCodec<TestModel2>());
        }

        private static void UnregisterTestCodecs()
        {
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.ClearCodecs(typeof(StreamEnd).Name);
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.ClearCodecs(new ModelKey(typeof(TestModel1)));
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.ClearCodecs(new ModelKey(typeof(TestModel2)));
        }

    }

}


