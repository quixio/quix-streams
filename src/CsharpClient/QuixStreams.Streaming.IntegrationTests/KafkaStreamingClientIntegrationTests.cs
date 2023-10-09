using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using FluentAssertions.Extensions;
using Quix.TestBase.Extensions;
using QuixStreams.Kafka;
using QuixStreams.Telemetry;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Kafka;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;
using RocksDbSharp;
using Xunit;
using Xunit.Abstractions;
using EventDefinition = QuixStreams.Telemetry.Models.EventDefinition;
using ParameterDefinition = QuixStreams.Telemetry.Models.ParameterDefinition;

namespace QuixStreams.Streaming.IntegrationTests
{
    [Collection("Kafka Container Collection")]
    public class KafkaStreamingClientIntegrationTests
    {
        private readonly ITestOutputHelper output;
        private readonly KafkaDockerTestFixture kafkaDockerTestFixture;
        private readonly KafkaStreamingClient client;

        public KafkaStreamingClientIntegrationTests(ITestOutputHelper output, KafkaDockerTestFixture kafkaDockerTestFixture)
        {
            this.output = output;
            this.kafkaDockerTestFixture = kafkaDockerTestFixture;
            QuixStreams.Logging.Factory = output.CreateLoggerFactory();
            client = new KafkaStreamingClient(kafkaDockerTestFixture.BrokerList, null);
            output.WriteLine($"Created client with brokerlist '{kafkaDockerTestFixture.BrokerList}'");
        }
        
        private static TimeseriesDataRaw GenerateTimeseriesData(int offset)
        {
            var vals = Enumerable.Range(0, 2).ToDictionary(k => "p" + k,
                s => Enumerable.Range(offset, 10).Select(s2 => new double?(s2 * 10.0)).ToArray());
            var stringVals = vals.ToDictionary(x => x.Key, x => x.Value.Select(y=> y.ToString()).ToArray());
            var binaryVals = stringVals.ToDictionary(x=> x.Key, x => x.Value.Select(y=> UTF8Encoding.UTF8.GetBytes(y)).ToArray());
            var epoch = 100000;
            return new TimeseriesDataRaw
            {
                Epoch = 0,
                Timestamps = Enumerable.Range(offset, 10).Select(s => (long)s + epoch).ToArray(),
                NumericValues = vals,
                StringValues = stringVals,
                BinaryValues = binaryVals
            };
        }
        
        

        [Fact]
        public async Task TopicSubUnSub_ShouldWorkAsExpected()
        {
            var topic = nameof(TopicSubUnSub_ShouldWorkAsExpected);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);

            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
            var topicProducer = client.GetTopicProducer(topic);

            IList<TimeseriesDataRaw> data = new List<TimeseriesDataRaw>();
            IList<EventDataRaw> events = new List<EventDataRaw>();
            var streamStarted = false;
            var streamEnded = false;
            StreamProperties streamProperties = null;
            var parameterDefinitionsChanged = false;
            var eventDefinitionsChanged = false;
            string streamId = null;

            topicConsumer.OnStreamReceived += (s, e) =>
            {
                if (e.StreamId != streamId)
                {
                    this.output.WriteLine("Ignoring stream {0}", e.StreamId);
                    return;
                }

                streamStarted = true;
            };

            topicConsumer.Subscribe();
            this.output.WriteLine("Subscribed");

            using (var stream = topicProducer.CreateStream())
            {
                streamId = stream.StreamId;
                this.output.WriteLine("First stream id is {0}", streamId);

                stream.Properties.Name = "Volvo car telemetry";
                stream.Properties.Flush();
                this.output.WriteLine("Flushing stream properties");
            }
            
            SpinWait.SpinUntil(() => streamStarted, 20000);
            streamStarted.Should().BeTrue();

            
            topicConsumer.Unsubscribe();
            this.output.WriteLine("Unsubscribed");

            streamStarted = false;
            using (var stream2 = topicProducer.CreateStream())
            {
                streamId = stream2.StreamId;
                this.output.WriteLine("Second stream id is {0}", streamId);

                stream2.Properties.Name = "Volvo car telemetry";
                stream2.Properties.Flush();
                this.output.WriteLine("Flushing stream properties");
            }

            SpinWait.SpinUntil(() => streamStarted, 5000);
            streamStarted.Should().BeFalse();
            

            topicConsumer.Subscribe();
            this.output.WriteLine("Subscribed (again)");
            SpinWait.SpinUntil(() => streamStarted, 20000);
            streamStarted.Should().BeTrue();


            topicConsumer.Dispose();
        }

        [Fact]
        public async Task StreamPublishAndConsume_ShouldReceiveExpectedMessages()
        {
            var topic = nameof(StreamPublishAndConsume_ShouldReceiveExpectedMessages);
            
            
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);
            
            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);

            IList<TimeseriesDataRaw> data = new List<TimeseriesDataRaw>();
            IList<EventDataRaw> events = new List<EventDataRaw>();
            var streamStarted = false;
            var streamEnded = false;
            StreamProperties streamProperties = null;
            var parameterDefinitionsChanged = false;
            var eventDefinitionsChanged = false;
            string streamId = null;

            topicConsumer.OnStreamReceived += (s, e) =>
            {
                if (e.StreamId != streamId)
                {
                    this.output.WriteLine("Ignoring stream {0}", e.StreamId);
                    return;
                }

                streamStarted = true;
                (e as IStreamConsumerInternal).OnTimeseriesData += (s2, e2) => data.Add(e2);
                (e as IStreamConsumerInternal).OnParameterDefinitionsChanged += (s2, e2) => parameterDefinitionsChanged = true;
                (e as IStreamConsumerInternal).OnEventDefinitionsChanged += (s2, e2) => eventDefinitionsChanged = true;
                (e as IStreamConsumerInternal).OnStreamPropertiesChanged += (s2, e2) => streamProperties = e2;
                (e as IStreamConsumerInternal).OnEventData += (s2, e2) => events.Add(e2);

                e.OnStreamClosed += (s2, e2) => streamEnded = true;
            };

            this.output.WriteLine("Subscribing");
            topicConsumer.Subscribe();
            this.output.WriteLine("Subscribed");

            using (var stream = topicProducer.CreateStream())
            {
                streamId = stream.StreamId;
                this.output.WriteLine("First stream id is {0}", streamId);

                stream.Properties.Name = "Volvo car telemetry";
                stream.Properties.Location = "Car telemetry/Vehicles/Volvo";
                stream.Properties.AddParent("1234");
                stream.Properties.Metadata["test_key"] = "test_value";
                stream.Properties.TimeOfRecording = new DateTime(2018, 01, 01);
                stream.Properties.Flush();
                this.output.WriteLine("Flushing stream properties");

                SpinWait.SpinUntil(() => streamStarted, 20000);
                Assert.True(streamStarted, "The stream failed to start");
                SpinWait.SpinUntil(() => streamProperties != null, 2000);

                Assert.True(streamStarted, "Stream is not started on reader.");
                Assert.Equal("Car telemetry/Vehicles/Volvo", streamProperties.Location);
                Assert.Equal("test_value", streamProperties.Metadata["test_key"]);
                Assert.Equal("1234", streamProperties.Parents.First());
                Assert.Equal(new DateTime(2018, 01, 01), streamProperties.TimeOfRecording);

                var expectedParameterDefinitions = new ParameterDefinitions
                {
                    Parameters = new List<ParameterDefinition>
                    {
                        {
                            new ParameterDefinition
                            {
                                Id = "p1",
                                Name = "P0 parameter",
                                MinimumValue = 0,
                                MaximumValue = 10,
                                Unit = "kmh"
                            }
                        },
                        {
                            new ParameterDefinition
                            {
                                Id = "p2",
                                Name = "P1 parameter",
                                MinimumValue = 0,
                                MaximumValue = 10,
                                Unit = "kmh"
                            }
                        }
                    }
                };

                var expectedEventDefinitions = new EventDefinitions
                {
                    Events = new List<EventDefinition>()
                    {
                        new EventDefinition
                        {
                            Id = "evid3"
                        },
                        new EventDefinition
                        {
                            Id = "evid4"
                        }
                    }
                };


                (stream as IStreamProducerInternal).Publish(expectedParameterDefinitions);
                this.output.WriteLine("Flushing parameter definitions");

                SpinWait.SpinUntil(() => parameterDefinitionsChanged, 2000);

                Assert.True(parameterDefinitionsChanged, "Parameter definitions event not reached reader.");

                (stream as IStreamProducerInternal).Publish(expectedEventDefinitions);
                this.output.WriteLine("Flushing event definitions");

                SpinWait.SpinUntil(() => eventDefinitionsChanged, 2000);

                Assert.True(eventDefinitionsChanged, "Event definitions event not reached reader.");

                this.output.WriteLine("Generating timeseries data raw");
                var expectedData = new List<TimeseriesDataRaw>();
                expectedData.Add(GenerateTimeseriesData(0));
                expectedData.Add(GenerateTimeseriesData(10));
                

                (stream as IStreamProducerInternal).Publish(expectedData);
                this.output.WriteLine("Publishing timeseries data raw");

                SpinWait.SpinUntil(() => data.Count == 2, 5000);

                Assert.Equal(2, data.Count);
                Assert.Equal(data.FirstOrDefault()?.Timestamps.FirstOrDefault(), expectedData.FirstOrDefault().Timestamps.FirstOrDefault());
                Assert.Equal(data.LastOrDefault()?.Timestamps.LastOrDefault(), expectedData.LastOrDefault().Timestamps.LastOrDefault());

                // test events
                var inputEvents = new EventDataRaw[]
                {
                    new EventDataRaw
                    {
                        Id = "abc",
                        Tags = new Dictionary<string, string>()
                        {
                            {"one", "two"}
                        },
                        Value = "Iamvalue",
                        Timestamp = 123456789
                    },
                    new EventDataRaw
                    {
                        Id = "efg",
                        Tags = new Dictionary<string, string>()
                        {
                            {"three", "fwo"}
                        },
                        Value = "Iamvalue2",
                        Timestamp = 123456790
                    }
                };
                // test single event
                (stream as IStreamProducerInternal).Publish(inputEvents[0]);
                SpinWait.SpinUntil(() => events.Count == 1, 2000);
                events[0].Should().BeEquivalentTo(inputEvents[0]);
                events.Clear();

                //test multiple event
                (stream as IStreamProducerInternal).Publish(inputEvents);
                SpinWait.SpinUntil(() => events.Count == 2, 2000);
                events.Should().BeEquivalentTo(inputEvents);

                stream.Close();

                SpinWait.SpinUntil(() => streamEnded, 2000);

                Assert.True(streamEnded, "Stream end event not reached reader.");
            }

            topicConsumer.Dispose();
        }

        [Fact]
        public async Task StreamPublishAndConsume_WithBuilder_ShouldReceiveExpectedMessages()
        {
            var topic = nameof(StreamPublishAndConsume_WithBuilder_ShouldReceiveExpectedMessages);
            
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);
            
            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);

            IList<TimeseriesDataRaw> data = new List<TimeseriesDataRaw>();
            IList<EventDataRaw> events = new List<EventDataRaw>();
            var streamStarted = false;
            var streamEnded = false;
            StreamProperties streamProperties = null;
            List<ParameterDefinition> parameterDefinitions = null;
            List<EventDefinition> eventDefinitions = null;
            string streamId = null;

            topicConsumer.OnStreamReceived += (s, e) =>
            {
                if (e.StreamId != streamId)
                {
                    return;
                }

                streamStarted = true;
                (e as IStreamConsumerInternal).OnTimeseriesData += (s2, e2) => data.Add(e2);
                (e as IStreamConsumerInternal).OnParameterDefinitionsChanged += (s2, e2) => parameterDefinitions = e2.Parameters ?? parameterDefinitions;
                (e as IStreamConsumerInternal).OnEventDefinitionsChanged += (s2, e2) => eventDefinitions = e2.Events ?? eventDefinitions;
                (e as IStreamConsumerInternal).OnStreamPropertiesChanged += (s2, e2) => streamProperties = e2;
                (e as IStreamConsumerInternal).OnEventData += (s2, e2) => events.Add(e2);

                e.OnStreamClosed += (s2, e2) => streamEnded = true;
            };

            topicConsumer.Subscribe();

            using (var stream = topicProducer.CreateStream())
            {
                streamId = stream.StreamId;
                output.WriteLine($"New stream created: {streamId}");

                stream.Properties.Name = "Volvo car telemetry";
                stream.Properties.Location = "Car telemetry/Vehicles/Volvo";
                stream.Properties.AddParent("1234");
                stream.Properties.Metadata["test_key"] = "test_value";

                SpinWait.SpinUntil(() => streamStarted, 20000);
                Assert.True(streamStarted, "Stream is not started on reader.");
                SpinWait.SpinUntil(() => streamProperties != null, 2000);

                Assert.Equal("Car telemetry/Vehicles/Volvo", streamProperties.Location);
                Assert.Equal("test_value", streamProperties.Metadata["test_key"]);
                Assert.Equal("1234", streamProperties.Parents.First());

                var expectedParameterDefinitions = new ParameterDefinitions
                {
                    Parameters = new List<ParameterDefinition>
                    {
                        {
                            new ParameterDefinition
                            {
                                Id = "p1",
                                Name = "P0 parameter",
                                Description = "Desc 1",
                                MinimumValue = 0,
                                MaximumValue = 10,
                                Unit = "kmh"
                            }
                        },
                        {
                            new ParameterDefinition
                            {
                                Id = "p2",
                                Name = "P1 parameter",
                                Description = "Desc 2",
                                MinimumValue = 0,
                                MaximumValue = 10,
                                Unit = "kmh"
                            }
                        }
                    },
                };

                var expectedEventDefinitions = new EventDefinitions
                {
                    Events = new List<EventDefinition>()
                    {
                        new EventDefinition
                        {
                            Id = "evid3",
                            Name = "evName3"
                        },
                        new EventDefinition
                        {
                            Id = "evid4",
                            Name = "evName4"
                        }
                    }
                };

                stream.Timeseries.AddDefinition("p1", "P0 parameter", "Desc 1").SetRange(0, 10).SetUnit("kmh");
                stream.Timeseries.AddDefinition("p2", "P1 parameter", "Desc 2").SetRange(0, 10).SetUnit("kmh");

                stream.Events.AddDefinition("evid3", "evName3");
                stream.Events.AddDefinition("evid4", "evName4");


                SpinWait.SpinUntil(() => parameterDefinitions != null && eventDefinitions != null, 2000);

                Assert.True(parameterDefinitions != null && eventDefinitions != null, "Parameter / Event definitions event not reached reader.");

                expectedParameterDefinitions.Parameters.Should().BeEquivalentTo(parameterDefinitions);
                expectedEventDefinitions.Events.Should().BeEquivalentTo(eventDefinitions);

                var expectedData = new List<TimeseriesDataRaw>();
                expectedData.Add(GenerateTimeseriesData(0));
                expectedData.Add(GenerateTimeseriesData(10));

                stream.Timeseries.Buffer.Epoch = ((long) 100000).FromUnixNanoseconds();
                stream.Timeseries.Buffer.PacketSize = 10;
                for (var i = 0; i < 20; i++)
                {
                    stream.Timeseries.Buffer.AddTimestampNanoseconds(i)
                        .AddValue("p0", i)
                        .AddValue("p1", i)
                        .AddValue("p2", i)
                        .Publish();
                }

                SpinWait.SpinUntil(() => data.Count == 2, 2000);

                Assert.Equal(2, data.Count);
                Assert.Equal(data.FirstOrDefault()?.Timestamps.FirstOrDefault(), expectedData.FirstOrDefault().Timestamps.FirstOrDefault());
                Assert.Equal(data.LastOrDefault()?.Timestamps.LastOrDefault(), expectedData.LastOrDefault().Timestamps.LastOrDefault());

                var now = DateTime.UtcNow;
                // test events
                var inputEvents = new EventDataRaw[]
                {
                    new EventDataRaw
                    {
                        Id = "abc",
                        Tags = new Dictionary<string, string>()
                        {
                            {"one", "two"}
                        },
                        Value = "Iamvalue",
                        Timestamp = 123456789
                    },
                    new EventDataRaw
                    {
                        Id = "efg",
                        Tags = new Dictionary<string, string>()
                        {
                            {"three", "fwo"}
                        },
                        Value = "Iamvalue2",
                        Timestamp = 123456790
                    },
                    new EventDataRaw
                    {
                        Id = "datetimetest",
                        Tags = new Dictionary<string, string>() { },
                        Value = "Iamvalue3",
                        Timestamp = now.ToUnixNanoseconds()
                    },
                    new EventDataRaw
                    {
                        Id = "timespan",
                        Tags = new Dictionary<string, string>() { },
                        Value = "Iamvalue4",
                        Timestamp = now.Add(TimeSpan.FromSeconds(10)).ToUnixNanoseconds()
                    }
                };



                // test single event
                stream.Events.AddTimestampNanoseconds(123456789)
                    .AddValue("abc", "Iamvalue")
                    .AddTag("one", "two")
                    .Publish();

                SpinWait.SpinUntil(() => events.Count == 1, 2000);
                events[0].Should().BeEquivalentTo(inputEvents[0]);
                events.Clear();

                //test multiple event
                stream.Events.AddTimestampNanoseconds(123456789)
                    .AddValue("abc", "Iamvalue")
                    .AddTag("one", "two")
                    .Publish();
                stream.Events.AddTimestampNanoseconds(123456790)
                    .AddValue("efg", "Iamvalue2")
                    .AddTag("three", "fwo")
                    .Publish();
                stream.Events.Epoch = now;
                stream.Events.AddTimestamp(now)
                    .AddValue("datetimetest", "Iamvalue3")
                    .Publish();
                stream.Events.AddTimestamp(TimeSpan.FromSeconds(10))
                    .AddValue("timespan", "Iamvalue4")
                    .Publish();

                SpinWait.SpinUntil(() => events.Count == 4, 2000);
                events.Should().BeEquivalentTo(inputEvents);

                stream.Close();

                SpinWait.SpinUntil(() => streamEnded, 2000);

                Assert.True(streamEnded, "Stream end event not reached reader.");

                topicConsumer.Dispose();
            }
        }

        [Fact]
        public async Task StreamRawReadAsQuix_ShouldReadMessageAsExpected()
        {
            var topic = nameof(StreamRawReadAsQuix_ShouldReadMessageAsExpected);

            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);
            
            // ** Consuming messages with Raw topic consumer
            var rawTopicConsumer = client.GetRawTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
            using var rawTopicProducer = client.GetRawTopicProducer(topic);
            
            var consumedKafkaMessages = new List<KafkaMessage>();
            
            var messageContent = $"lorem ipsum - event";
            var messageContentInBytes = Encoding.UTF8.GetBytes(messageContent);

            rawTopicConsumer.OnMessageReceived += (s, rawMessage) =>
            {
                this.output.WriteLine($"Raw consumer received: {Encoding.UTF8.GetString(rawMessage.Value)}");
                if (!rawMessage.Value.SequenceEqual(messageContentInBytes))
                {
                    this.output.WriteLine("Ignoring message");
                    return;
                }
                consumedKafkaMessages.Add(rawMessage);
            };

            this.output.WriteLine("Subscribing");
            rawTopicConsumer.Subscribe();
            this.output.WriteLine("Unsubscribing");
            rawTopicConsumer.Unsubscribe(); // Cheap test for now to test unsub/sub
            this.output.WriteLine("Subscribing (again)");
            rawTopicConsumer.Subscribe(); // Cheap test for now to test unsub/sub


            rawTopicProducer.Publish(new KafkaMessage(null, messageContentInBytes));
            
            SpinWait.SpinUntil(() => consumedKafkaMessages.Count >= 1, 10000);
            try
            {
                consumedKafkaMessages.Should().ContainSingle();
                consumedKafkaMessages.Where(x => x.Key == null).Should().ContainSingle();
            }
            finally
            {
                // To make sure the consumer disconnects and doesn't fight with next attempt for partition
                rawTopicConsumer.Dispose();
            }


            // ** Consuming messages with Event topic consumer
            messageContent = $"lorem ipsum - event";
            messageContentInBytes = Encoding.UTF8.GetBytes(messageContent);

            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            
            var consumedEvents = new List<EventDataRaw>();

            topicConsumer.OnStreamReceived += (s, e) =>
            {
                (e as IStreamConsumerInternal).OnEventData += (s, eventData) =>
                {
                    this.output.WriteLine($"Event consumer received message: {eventData.Value}");
                    if (eventData.Value != messageContent)
                    {
                        this.output.WriteLine("Ignoring message");
                        return;
                    }

                    consumedEvents.Add(eventData);
                };
            };

            this.output.WriteLine("Subscribing");
            topicConsumer.Subscribe();
            this.output.WriteLine("Unsubscribing");
            topicConsumer.Unsubscribe(); // Cheap test for now to test unsub/sub
            this.output.WriteLine("Subscribing (again)");
            topicConsumer.Subscribe(); // Cheap test for now to test unsub/sub
            
            rawTopicProducer.Publish(new KafkaMessage(key: Encoding.UTF8.GetBytes("some key"), value: messageContentInBytes));
            rawTopicProducer.Publish(new KafkaMessage(key: null, value: messageContentInBytes));

            SpinWait.SpinUntil(() => consumedEvents.Count >= 2, 10000);
            try
            {
                consumedEvents.Count.Should().Be(2);
                consumedEvents.Where(x => x.Id == "some key").Should().ContainSingle();
                consumedEvents.Where(x => x.Id == StreamPipeline.DefaultStreamIdWhenMissing).Should().ContainSingle();
            }
            finally
            {
                // To make sure the consumer disconnects and doesn't fight with next attempt for partition
                topicConsumer.Dispose();   
            }
            
        }

        [Fact]
        public async Task StreamCloseAndReopenSameStream_ShouldRaiseEventAsExpected()
        {
            var topic = nameof(StreamCloseAndReopenSameStream_ShouldRaiseEventAsExpected);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);

            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);

            IList<TimeseriesDataRaw> data = new List<TimeseriesDataRaw>();
            IList<EventDataRaw> events = new List<EventDataRaw>();
            var streamStarted = false;
            var streamEnded = false;
            string streamId = null;

            topicConsumer.OnStreamReceived += (s, e) =>
            {
                if (e.StreamId != streamId)
                {
                    this.output.WriteLine("Ignoring stream {0}", e.StreamId);
                    return;
                }

                streamStarted = true;
                e.Timeseries.OnRawReceived += (s2, e2) => data.Add(e2.Data);
                e.OnStreamClosed += (s2, e2) => streamEnded = true;
            };

            this.output.WriteLine("Subscribing");
            topicConsumer.Subscribe();
            this.output.WriteLine("Subscribed");

            using (var stream = topicProducer.CreateStream())
            {
                streamId = stream.StreamId;
                this.output.WriteLine("First stream id is {0}", streamId);

                stream.Timeseries.Buffer.AddTimestampNanoseconds(100)
                    .AddValue("p0", 1)
                    .AddValue("p1", 2)
                    .AddValue("p2", 3)
                    .Publish();
                
                this.output.WriteLine("Published data for {0}, waiting for stream to start", streamId);

                SpinWait.SpinUntil(() => streamStarted, 20000);
                Assert.True(streamStarted, "First stream is not started on reader.");
                this.output.WriteLine("First stream started");

                SpinWait.SpinUntil(() => data.Count == 1, 2000);

                Assert.Equal(1, data.Count);


                // Close stream
                stream.Close();

                SpinWait.SpinUntil(() => streamEnded, 2000);

                Assert.True(streamEnded, "First stream did not close in time.");

                streamStarted = false;

                // Second stream
                using (var stream2 = topicProducer.CreateStream(streamId))
                {
                    stream2.Timeseries.Buffer.AddTimestampNanoseconds(100)
                        .AddValue("p0", 1)
                        .AddValue("p1", 2)
                        .AddValue("p2", 3)
                        .Publish();
                }

                SpinWait.SpinUntil(() => streamStarted, 2000);

                Assert.True(streamStarted, "Second stream is not started on reader.");
            }

            topicConsumer.Dispose();
        }
        
        [Fact]
        public async Task StreamGetOrCreateStream_ShouldNotThrowException()
        {
            var topic = nameof(StreamGetOrCreateStream_ShouldNotThrowException);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);

            var topicProducer = client.GetTopicProducer(topic);
            var result = Parallel.For(0, 1000, parallelOptions: new ParallelOptions()
            {
                MaxDegreeOfParallelism = 20
            }, (loop) => { topicProducer.GetOrCreateStream("testme"); });

            result.IsCompleted.Should().BeTrue();
            var stream = topicProducer.GetOrCreateStream("testme");
            var stream2 = topicProducer.GetOrCreateStream("testme");
            stream.Should().BeSameAs(stream2);
        }
        
        [Fact]
        public async Task ReadingAStreamWithSameConsumerGroup_ShouldGetRevokedOnOne()
        {
            // Arrange
            var topic = nameof(ReadingAStreamWithSameConsumerGroup_ShouldGetRevokedOnOne);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);

            var consumerGroup = "doesntmatteraslongassame";
            var topicConsumer1 = client.GetTopicConsumer(topic, consumerGroup, autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);

            var expectedStreamCount = 1;
            long streamsRevoked = 0;
            long streamsReceived = 0;
            var cts = new CancellationTokenSource();

            var streams = new List<IStreamProducer>();
            for (var i = 0; i <= expectedStreamCount; i++)
            {
                streams.Add(topicProducer.CreateStream());
            }

            topicConsumer1.OnStreamReceived += (sender, sr) =>
            {
                Interlocked.Increment(ref streamsReceived);
            };
            topicConsumer1.OnStreamsRevoked += (sender, streams) =>
            {
                Interlocked.Add(ref streamsRevoked, streams.Length);
            };
            topicConsumer1.Subscribe();
            
            var writerTask = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    this.output.WriteLine("Generating data for streams");
                    foreach (var stream in streams)
                    {
                        stream.Timeseries.Buffer.AddTimestamp(DateTime.UtcNow)
                            .AddValue("p0", 1)
                            .Publish();
                        stream.Timeseries.Buffer.Flush();
                    }

                    await Task.Delay(1000, cts.Token);
                }
            });
            
            SpinWait.SpinUntil(() => streamsReceived > 0, TimeSpan.FromSeconds(20));
            streamsReceived.Should().BeGreaterThan(0);

            // Act 
            // Add additional consumers to force kafka a rebalance
            long streamsReceived2 = 0;
            for (var index = 0; index <= 5; index++)
            {
                if (streamsReceived2 > 0) break;
                var timer = Stopwatch.StartNew();
                var consumerToStealPartition = client.GetTopicConsumer(topic, consumerGroup, autoOffset: AutoOffsetReset.Latest);
                consumerToStealPartition.OnStreamReceived += (sender, sr) => { Interlocked.Increment(ref streamsReceived2); };
                consumerToStealPartition.Subscribe();
                this.output.WriteLine("Took {0:g} to start a new consumer", timer.Elapsed);
            }

            SpinWait.SpinUntil(() => streamsReceived2 > 0, TimeSpan.FromSeconds(20));
            streamsReceived2.Should().BeGreaterThan(0);
            SpinWait.SpinUntil(() => streamsRevoked > 0, TimeSpan.FromSeconds(10));
            streamsRevoked.Should().BeGreaterThan(0, $"found {streamsReceived2} streams from new consumer but original hasn't revoked");
        }

        [Fact]
        public async Task StreamState_ShouldWorkAsExpected()
        {
            var topic = nameof(StreamState_ShouldWorkAsExpected);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);

            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);
            
            var msgCounter = 0;
            topicConsumer.OnStreamReceived += (sender, stream) =>
            {
                // Clean previous run
                stream.GetStateManager().DeleteStates();

                var rollingSum = stream.GetScalarState("RollingSumTotal", (sid) => 0d);
                var rollingSumPerParameter = stream.GetDictionaryState("RollingSum", (sid) => 0d);

                stream.Timeseries.OnDataReceived += (o, args) =>
                {
                    foreach (var data in args.Data.Timestamps)
                    {
                        foreach (var parameter in data.Parameters)
                        {
                            if (parameter.Value.Type == ParameterValueType.Numeric)
                            {
                                rollingSumPerParameter[parameter.Key] += parameter.Value.NumericValue ?? 0;
                                rollingSum.Value += parameter.Value.NumericValue ?? 0;

                                this.output.WriteLine($"Rolling sum for {parameter.Key} is {rollingSumPerParameter[parameter.Key]}");
                            }  
                        }
                    }
                    
                    msgCounter++;
                };
            };
            
            topicConsumer.Subscribe();

            var start = DateTime.UtcNow;
            var streamProducer = topicProducer.GetOrCreateStream("stream1");
            streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(1)).AddValue("param1", 5).Publish();
            streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(2)).AddValue("param2", 10).Publish();
            streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(3)).AddValue("param1", 9).Publish();
            streamProducer.Timeseries.Flush();
            //streamProducer.Close();
            
            var streamProducer2 = topicProducer.GetOrCreateStream("stream2");
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(1)).AddValue("param1", 5).Publish();
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(2)).AddValue("param2", 7).Publish();
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(3)).AddValue("param1", 4).Publish();
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(4)).AddValue("param2", 3).Publish();
            streamProducer2.Timeseries.Flush();
            //streamProducer2.Close();

            topicProducer.Dispose();
            output.WriteLine("Closed Producer");

            // Wait for enough messages to be received
            
            output.WriteLine("Waiting for messages");
            SpinWait.SpinUntil(() => msgCounter == 7, TimeSpan.FromSeconds(10000));
            output.WriteLine($"Waited for messages, got {msgCounter}");
            
            msgCounter.Should().Be(7);
            output.WriteLine($"Got expected number of messages");
            topicConsumer.Commit();

            output.WriteLine($"Checking Stream 1 Rolling sum for params");
            var stream1StateRollingSum = topicConsumer.GetStreamStateManager(streamProducer.StreamId).GetScalarState<double>("RollingSumTotal");
            var stream1StateRollingSumPerParam = topicConsumer.GetStreamStateManager(streamProducer.StreamId).GetDictionaryState<double>("RollingSum");
            stream1StateRollingSumPerParam["param1"].Should().Be(14);
            stream1StateRollingSumPerParam["param2"].Should().Be(10);
            stream1StateRollingSum.Value.Should().Be(24);
            output.WriteLine($"Checked Stream 1 Rolling sum for params");
            output.WriteLine($"Checking Stream 2 Rolling sum for params");
            var stream2StateRollingSum = topicConsumer.GetStreamStateManager(streamProducer2.StreamId).GetScalarState<double>("RollingSumTotal");
            var stream2StateRollingSumPerParam = topicConsumer.GetStreamStateManager(streamProducer2.StreamId).GetDictionaryState<double>("RollingSum");
            stream2StateRollingSumPerParam["param1"].Should().Be(9);
            stream2StateRollingSumPerParam["param2"].Should().Be(10);
            stream2StateRollingSum.Value.Should().Be(19);
            output.WriteLine($"Checked Stream 2 Rolling sum for params");
            topicConsumer.Dispose();
        }
        
        [Fact]
        public async Task StreamState_CommittedFromAnotherThread_ShouldWorkAsExpected()
        {
            var topic = nameof(StreamState_CommittedFromAnotherThread_ShouldWorkAsExpected);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);
            
            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);

            var testLength = TimeSpan.FromSeconds(10);
            var mre = new ManualResetEvent(false);

            var exceptionOccurred = false;

            var msgCounter = 0;
            topicConsumer.OnStreamReceived += (sender, stream) =>
            {
                // Clean previous run
                stream.GetStateManager().DeleteStates();
                
                mre.Set();
                stream.Timeseries.OnDataReceived += (o, args) =>
                {
                    var rollingSum = stream.GetDictionaryState("RollingSum", (sid) => 0d);
                    try
                    {
                        foreach (var data in args.Data.Timestamps)
                        {
                            foreach (var parameter in data.Parameters)
                            {
                                if (parameter.Value.Type == ParameterValueType.Numeric)
                                {
                                    rollingSum[parameter.Key] += parameter.Value.NumericValue ?? 0;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptionOccurred = true;
                        this.output.WriteLine($"Exception while consuming{Environment.NewLine}{ex}");
                    }

                    msgCounter++;
                };
            };
            
            topicConsumer.Subscribe();
            
            var start = DateTime.UtcNow;
            var streamProducer = topicProducer.GetOrCreateStream("stream1");
            streamProducer.Properties.Name = "test";
            streamProducer.Flush();

            if (!mre.WaitOne(TimeSpan.FromSeconds(10))) throw new Exception("Did not receive stream in time");

            var end = DateTime.UtcNow.Add(testLength);

            void BackgroundCommitter()
            {
                try
                {
                    while (!exceptionOccurred && DateTime.UtcNow < end)
                    {
                        topicConsumer.Commit();
                        Thread.Sleep(3);
                    }
                }
                catch (Exception ex)
                {
                    exceptionOccurred = true;
                    this.output.WriteLine($"Exception while committing{Environment.NewLine}{ex}");
                }
            }
            
            var iteration = 0;
            void BackgroundSender()
            {
                try
                {
                    while (!exceptionOccurred && DateTime.UtcNow < end)
                    {
                        iteration++;
                        streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(iteration))
                            .AddValue("param1", iteration).Publish();
                        if (iteration % 5 == 0) // to create a bit bigger batches
                        {
                            streamProducer.Timeseries.Buffer.Flush();
                            Thread.Sleep(6); // to avoid completely hammering underlying kafka
                        }
                    }
                }
                catch (Exception ex)
                {
                    exceptionOccurred = true;
                    this.output.WriteLine($"Exception while sending {Environment.NewLine}{ex}");

                }
            }

            var senderTask = Task.Run(BackgroundSender);
            var committerTask = Task.Run(BackgroundCommitter);

            Task.WaitAll(senderTask, committerTask);
            
            streamProducer.Timeseries.Flush();
            this.output.WriteLine($"Wrote {iteration} iteration");
            exceptionOccurred.Should().BeFalse();
        }

        [Fact]
        public async Task StreamState_ShouldWorkOnRebalancing()
        {
            var topic = nameof(StreamState_ShouldWorkOnRebalancing);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 2);

            var topicConsumer = client.GetTopicConsumer(topic, "same_group", autoOffset: AutoOffsetReset.Latest);
            var topicConsumer2 = client.GetTopicConsumer(topic, "same_group", autoOffset: AutoOffsetReset.Latest);
            
            var topicProducer = client.GetTopicProducer(topic);
            
            var msgCounter = 0;

            void StreamReceived(object sender, IStreamConsumer stream)
            {
                // Clean previous run
                stream.GetStateManager().DeleteStates();

                var rollingSum = stream.GetScalarState("RollingSumTotal", (sid) => 0d);

                stream.Timeseries.OnDataReceived += (o, args) =>
                {
                    foreach (var data in args.Data.Timestamps)
                    {
                        foreach (var parameter in data.Parameters)
                        {
                            if (parameter.Value.Type == ParameterValueType.Numeric)
                            {
                                rollingSum.Value += parameter.Value.NumericValue ?? 0;

                                this.output.WriteLine($"Rolling sum is {rollingSum.Value}");
                            }
                        }
                    }

                    msgCounter++;
                };
            }

            topicConsumer.OnStreamReceived += StreamReceived;
            topicConsumer2.OnStreamReceived += StreamReceived;

            topicConsumer.Subscribe();

            var start = DateTime.UtcNow;
            var streamProducer = topicProducer.GetOrCreateStream("stream1");
            streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(1)).AddValue("param1", 1).Publish();
            streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(2)).AddValue("param2", 1).Publish();
            streamProducer.Timeseries.Flush();
            //streamProducer.Close();
            
            var streamProducer2 = topicProducer.GetOrCreateStream("stream2");
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(1)).AddValue("param1", 5).Publish();
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(2)).AddValue("param2", 5).Publish();
            streamProducer2.Timeseries.Flush();
            //streamProducer2.Close();

            topicConsumer2.Subscribe();
            
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(1)).AddValue("param1", 10).Publish();
            streamProducer2.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(2)).AddValue("param2", 10).Publish();
            streamProducer2.Timeseries.Flush();
            
            topicProducer.Dispose();
            output.WriteLine("Closed Producer");
            
            // Wait for enough messages to be received
            output.WriteLine("Waiting for messages");
            SpinWait.SpinUntil(() => msgCounter == 6, TimeSpan.FromSeconds(10000));
            output.WriteLine($"Waited for messages, got {msgCounter}");
            
            msgCounter.Should().Be(6);
            output.WriteLine($"Got expected number of messages");
            
            topicConsumer.Commit();

            output.WriteLine($"Checking Stream 1 Rolling sum for params");
            var stream1StateRollingSum = topicConsumer.GetStreamStateManager(streamProducer.StreamId).GetScalarState<double>("RollingSumTotal");
            stream1StateRollingSum.Value.Should().Be(2);
            output.WriteLine($"Checked Stream 1 Rolling sum for params");
            output.WriteLine($"Checking Stream 2 Rolling sum for params");
            var stream2StateRollingSum = topicConsumer.GetStreamStateManager(streamProducer2.StreamId).GetScalarState<double>("RollingSumTotal");
            stream2StateRollingSum.Value.Should().Be(30);
            output.WriteLine($"Checked Stream 2 Rolling sum for params");
            
            topicConsumer.Dispose();
            topicConsumer2.Dispose();
        }
        
        [Fact]
        public async Task StreamState_RocksDbDatabaseDisposedOnStreamRevoke()
        {
            var topic = nameof(StreamState_RocksDbDatabaseDisposedOnStreamRevoke);
            await this.kafkaDockerTestFixture.EnsureTopic(topic, 1);

            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Latest);
            var topicProducer = client.GetTopicProducer(topic);
            
            var msgCounter = 0;
            topicConsumer.OnStreamReceived += (sender, stream) =>
            {
                // Clean previous run
                stream.GetStateManager().DeleteStates();
                stream.GetScalarState("RandomScalar", (sid) => 0d);

                stream.Timeseries.OnDataReceived += (o, args) =>
                {
                    msgCounter++;
                };
            };
            
            topicConsumer.Subscribe();

            var start = DateTime.UtcNow;
            var streamName = "stream1";
            var streamProducer = topicProducer.GetOrCreateStream(streamName);
            streamProducer.Timeseries.Buffer.AddTimestamp(start.AddMicroseconds(1)).AddValue("param1", 5).Publish();
            streamProducer.Timeseries.Flush();

            topicProducer.Dispose();
            output.WriteLine("Closed Producer");

            // Wait for enough messages to be received
            
            output.WriteLine("Waiting for messages");
            SpinWait.SpinUntil(() => msgCounter == 1, TimeSpan.FromSeconds(10000));
            output.WriteLine($"Waited for messages, got {msgCounter}");
            
            topicConsumer.Commit();
            var storageDir = topicConsumer.GetStreamStateManager(streamName).StorageDir;
            var openRocksDBinSecondProcessTask = Task.Run(() => AttemptToOpenRocksDb(storageDir));
            
            openRocksDBinSecondProcessTask.Result.Should().BeFalse("because the second process shouldn't be able to open a RocksDB connection, as one is already open at the same location.");
            
            topicConsumer.Dispose();
            
            openRocksDBinSecondProcessTask = Task.Run(() => AttemptToOpenRocksDb(storageDir));

            openRocksDBinSecondProcessTask.Result.Should().BeTrue("because the second process should be able to open a RocksDB connection, as the connection of the first db was disposed.");

            
            bool AttemptToOpenRocksDb(string dbPath)
            {
                try
                {
                    using (RocksDb.Open(new DbOptions(), dbPath))
                    {
                        // This code path means that it was able to access the DB
                        return true;
                    }
                }
                catch (RocksDbException ex)
                {
                    // Returns false if the exception message contains the keyword "lock".
                    return !ex.Message.Contains("lock");
                }
            }
        }
        
        
    }
}