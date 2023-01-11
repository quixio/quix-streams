using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Process.Common.Test;
using Quix.Sdk.Process.UnitTests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Xunit;
using Quix.Sdk.Process.Models;
using Quix.TestBase.Extensions;
using Xunit.Abstractions;

namespace Quix.Sdk.Process.UnitTests
{
    public class KafkaReaderWriterShould
    {
        public KafkaReaderWriterShould(ITestOutputHelper helper)
        {
            Quix.Sdk.Logging.Factory = helper.CreateLoggerFactory();
        }
        
        [Fact]
        public void KafkaReaderWriter_AfterSendSeveralStreams_ShouldReadProperStreams()
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

            // Create Kafka Reader
            var kafkaReader = new TestKafkaReader(testBroker);
            kafkaReader.ForEach(streamId =>
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

            kafkaReader.Start();

            // Create streams
            var stream1 = new StreamProcess()
                .AddComponent(new TestKafkaWriter(testBroker, "StreamId_1"));
            var stream2 = new StreamProcess()
                .AddComponent(new TestKafkaWriter(testBroker, "StreamId_2"));
            var stream3 = new StreamProcess()
                .AddComponent(new TestKafkaWriter(testBroker, "StreamId_3"));

            // ACT
            stream1.Send(testModel1);
            stream2.Send(testModel1);
            stream2.Send(testModel2);
            stream3.Send(testModel2);

            // ASSERT
            Assert.Equal(3, kafkaReader.ContextCache.GetAll().Count);
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
            Assert.Equal(2, kafkaReader.ContextCache.GetAll().Count);

            // ACT - RE-OPEN TEST
            streamStarted = false;
            stream1.Send(testModel1);

            // ASSERT RE-OPEN TEST
            Assert.True(streamStarted);
            Assert.Equal(3, kafkaReader.ContextCache.GetAll().Count);
        }

        [Fact]
        public void KafkaReaderWriter_WithoutRegisterCodecs_ShouldRaiseExceptions()
        {
            UnregisterTestCodecs();

            // ARRANGE
            TestBroker testBroker = new TestBroker();
            TestModel1 testModel1 = new TestModel1();
            bool raised = false;

            // Create Kafka Reader
            KafkaReader kafkaReader = new TestKafkaReader(testBroker);
            kafkaReader.ForEach(streamId =>
            {
                var s = new StreamProcess(streamId);
                return s;
            });

            // Create Kafka Writer
            KafkaWriter kafkaWriter = new TestKafkaWriter(testBroker, "StreamId_1");
            var stream1 = new StreamProcess()
                .AddComponent(kafkaWriter);

            kafkaWriter.OnWriteException += (sender, e) => raised = true;

            // ACT
            stream1.Send(testModel1);

            // ASSERT
            Assert.True(raised);
        }

        [Fact]
        public void KafkaReaderWriter_WithErrorsOnSend_ShouldRaiseExceptions()
        {
            RegisterTestCodecs();

            // ARRANGE
            TestBroker testBroker = new TestBroker(true);
            TestModel1 testModel1 = new TestModel1();
            bool raised = false;

            // Create Kafka Reader
            KafkaReader kafkaReader = new TestKafkaReader(testBroker);
            kafkaReader.ForEach(streamId =>
            {
                var s = new StreamProcess(streamId);
                return s;
            });

            // Create Kafka Writer
            KafkaWriter kafkaWriter = new TestKafkaWriter(testBroker, "StreamId_1");
            var stream1 = new StreamProcess()
                .AddComponent(kafkaWriter);

            kafkaWriter.OnWriteException += (sender, e) => raised = true;

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


