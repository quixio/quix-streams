using System;
using System.Collections.Generic;
using Quix.Streams.Telemetry.Common.Test;
using Quix.Streams.Telemetry.Kafka;
using Quix.Streams.Telemetry.Models;
using Quix.Streams.Telemetry.UnitTests.Helpers;
using Quix.TestBase.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Quix.Streams.Telemetry.UnitTests
{
    public class KafkaConsumerProducerShould
    {
        public KafkaConsumerProducerShould(ITestOutputHelper helper)
        {
            Quix.Streams.Logging.Factory = helper.CreateLoggerFactory();
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
            var kafkaConsumer = new TestTelemetryKafkaConsumer(testBroker);
            kafkaConsumer.ForEach(streamId =>
            {
                streamStarted = true;

                var s = new StreamProcess(streamId);
                s.Subscribe((streamProcess, package) =>
                {
                    results.Add((streamProcess.StreamId, package.Type));
                });
                s.Subscribe<TestModel1>((streamProcess, model) =>
                {
                    resultsModel1.Add((streamProcess.StreamId, model.Id));
                });
                s.Subscribe<TestModel2>((streamProcess, model) =>
                {
                    resultsModel2.Add((streamProcess.StreamId, model.Id));
                });
                return s;
            });

            kafkaConsumer.Start();

            // Create streams
            var stream1 = new StreamProcess()
                .AddComponent(new TestTelemetryKafkaProducer(testBroker, "StreamId_1"));
            var stream2 = new StreamProcess()
                .AddComponent(new TestTelemetryKafkaProducer(testBroker, "StreamId_2"));
            var stream3 = new StreamProcess()
                .AddComponent(new TestTelemetryKafkaProducer(testBroker, "StreamId_3"));

            // ACT
            stream1.Send(testModel1);
            stream2.Send(testModel1);
            stream2.Send(testModel2);
            stream3.Send(testModel2);

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
            stream1.Send(streamEnd);

            // ASSERT
            Assert.Equal(2, kafkaConsumer.ContextCache.GetAll().Count);

            // ACT - RE-OPEN TEST
            streamStarted = false;
            stream1.Send(testModel1);

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
                var s = new StreamProcess(streamId);
                return s;
            });

            // Create Kafka producer
            TelemetryKafkaProducer telemetryKafkaProducer = new TestTelemetryKafkaProducer(testBroker, "StreamId_1");
            var stream1 = new StreamProcess()
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
                var s = new StreamProcess(streamId);
                return s;
            });

            // Create Kafka producer
            TelemetryKafkaProducer telemetryKafkaProducer = new TestTelemetryKafkaProducer(testBroker, "StreamId_1");
            var stream1 = new StreamProcess()
                .AddComponent(telemetryKafkaProducer);

            telemetryKafkaProducer.OnWriteException += (sender, e) => raised = true;

            // ACT
            stream1.Send(testModel1);

            // ASSERT
            Assert.True(raised);
        }

        private static void RegisterTestCodecs()
        {
            Transport.Registry.CodecRegistry.RegisterCodec(typeof(StreamEnd).Name, new Transport.Fw.Codecs.DefaultJsonCodec<StreamEnd>());
            Transport.Registry.CodecRegistry.RegisterCodec(new Transport.Fw.ModelKey(typeof(TestModel1)), new Transport.Fw.Codecs.DefaultJsonCodec<TestModel1>());
            Transport.Registry.CodecRegistry.RegisterCodec(new Transport.Fw.ModelKey(typeof(TestModel2)), new Transport.Fw.Codecs.DefaultJsonCodec<TestModel2>());
        }

        private static void UnregisterTestCodecs()
        {
            Transport.Registry.CodecRegistry.ClearCodecs(typeof(StreamEnd).Name);
            Transport.Registry.CodecRegistry.ClearCodecs(new Transport.Fw.ModelKey(typeof(TestModel1)));
            Transport.Registry.CodecRegistry.ClearCodecs(new Transport.Fw.ModelKey(typeof(TestModel2)));
        }

    }

}


