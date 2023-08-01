using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Quix.TestBase.Extensions;
using QuixStreams.State.Storage;
using QuixStreams.State.Storage.FileStorage.LocalFileStorage;
using QuixStreams.Telemetry;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.Raw;
using QuixStreams.Streaming.States;
using QuixStreams.Telemetry.Kafka;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;
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
        private readonly KafkaStreamingClient client;
        private int MaxTestRetry = 3;

        public KafkaStreamingClientIntegrationTests(ITestOutputHelper output, KafkaDockerTestFixture kafkaDockerTestFixture)
        {
            this.output = output;
            QuixStreams.Logging.Factory = output.CreateLoggerFactory();
            client = new KafkaStreamingClient(kafkaDockerTestFixture.BrokerList, kafkaDockerTestFixture.SecurityOptions);
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
        public void StreamReadAndWrite()
        {
            var topic = nameof(StreamReadAndWrite);
            RunTest(() =>
            {
                // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
                // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
                // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
                // created before our consumer tried to connect.
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
            });
        }

        [Fact]
        public void StreamReadAndWriteBuilders()
        {
            var topic = nameof(StreamReadAndWriteBuilders);
            RunTest(() =>
            {
                // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
                // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
                // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
                // created before our consumer tried to connect.
                var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
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
            });
        }

        [Fact]
        public void StreamReadAndWriteRaw()
        {
            var topic = nameof(StreamReadAndWriteRaw);

            var attempt = 0;
            RunTest(() =>
            {
                attempt++;
                var messageContent = $"lorem ipsum - raw {attempt}";
                var messageContentInBytes = Encoding.UTF8.GetBytes(messageContent);

                // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
                // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
                // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
                // created before our consumer tried to connect.
                var rawTopicConsumer = client.GetRawTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
                var rawTopicProducer = client.GetRawTopicProducer(topic);

                // ** Consuming messages with Raw topic consumer

                var consumedRawMessages = new List<RawMessage>();

                rawTopicConsumer.OnMessageReceived += (s, rawMessage) =>
                {
                    this.output.WriteLine($"Raw consumer received: {Encoding.UTF8.GetString(rawMessage.Value)}");
                    if (!rawMessage.Value.SequenceEqual(messageContentInBytes))
                    {
                        this.output.WriteLine("Ignoring message");
                        return;
                    }
                    consumedRawMessages.Add(rawMessage);
                };

                rawTopicConsumer.Subscribe();

                rawTopicProducer.Publish(new RawMessage(messageContentInBytes));
                
                SpinWait.SpinUntil(() => consumedRawMessages.Count >= 1, 10000);
                try
                {
                    consumedRawMessages.Should().ContainSingle();
                    consumedRawMessages.Where(x => x.Key == null).Should().ContainSingle();
                }
                finally
                {
                    // To make sure the consumer disconnects and doesn't fight with next attempt for partition
                    
                    rawTopicConsumer.Dispose();
                }


                // ** Consuming messages with Event topic consumer

                messageContent = $"lorem ipsum - event {attempt}";
                messageContentInBytes = Encoding.UTF8.GetBytes(messageContent);

                var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
                
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

                topicConsumer.Subscribe();

                rawTopicProducer.Publish(new RawMessage(key: "some key"u8.ToArray(), value: messageContentInBytes));
                rawTopicProducer.Publish(new RawMessage(key: null, value: messageContentInBytes));

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
            });
        }

        [Fact]
        public void StreamCloseAndReopenSameStream_ShouldRaiseEventAsExpected()
        {
            var topic = nameof(StreamCloseAndReopenSameStream_ShouldRaiseEventAsExpected);
            RunTest(() =>
            {
                // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
                // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
                // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
                // created before our consumer tried to connect.
                var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
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
                    (e as IStreamConsumerInternal).OnTimeseriesData += (s2, e2) => data.Add(e2);
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
            });
        }
        
        [Fact]
        public void StreamGetOrCreateStream_ShouldNotThrowException()
        {
            var topic = nameof(StreamGetOrCreateStream_ShouldNotThrowException);
            RunTest(() =>
            {
                var topicProducer = client.GetTopicProducer(topic);
                var result = Parallel.For(0, 1000, parallelOptions: new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 20
                }, (loop) => { topicProducer.GetOrCreateStream("testme"); });

                result.IsCompleted.Should().BeTrue();
                var stream = topicProducer.GetOrCreateStream("testme");
                var stream2 = topicProducer.GetOrCreateStream("testme");
                stream.Should().BeSameAs(stream2);
            });
        }
        
        [Fact]
        public void ReadingAStreamWithSameConsumerGroup_ShouldGetRevokedOnOne()
        {
            var topic = nameof(ReadingAStreamWithSameConsumerGroup_ShouldGetRevokedOnOne);
            RunTest(() =>
            {
                // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
                // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
                // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
                // created before our consumer tried to connect.
                var consumerGroup = "doesntmatteraslongassame";
                var topicConsumer1 = client.GetTopicConsumer(topic, consumerGroup, autoOffset: AutoOffsetReset.Earliest);
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

                topicConsumer1.OnStreamReceived += (sender, sr) =>
                {
                    Interlocked.Increment(ref streamsReceived);
                };
                topicConsumer1.OnStreamsRevoked += (sender, streams) =>
                {
                    foreach (var streamConsumer in streams)
                    {
                        Interlocked.Increment(ref streamsRevoked);
                    }
                };
                topicConsumer1.Subscribe();
                SpinWait.SpinUntil(() => streamsReceived > 0, TimeSpan.FromSeconds(20));
                streamsReceived.Should().BeGreaterThan(0);



                long streamsReceived2 = 0;
                for (var index = 0; index <= 5; index++)
                {
                    if (streamsReceived2 > 0) break;
                    var topicConsumer2 = client.GetTopicConsumer(topic, consumerGroup, autoOffset: AutoOffsetReset.Latest);
                    topicConsumer2.OnStreamReceived += (sender, sr) => { Interlocked.Increment(ref streamsReceived2); };
                    topicConsumer2.Subscribe();
                }

                SpinWait.SpinUntil(() => streamsReceived2 > 0, TimeSpan.FromSeconds(20));
                streamsReceived2.Should().BeGreaterThan(0);
                SpinWait.SpinUntil(() => streamsRevoked > 0, TimeSpan.FromSeconds(10000));
                streamsRevoked.Should().BeGreaterThan(0);
            });
        }

        [Fact]
        public void StreamState_ShouldWorkAsExpected()
        {
            var topic = nameof(StreamState_ShouldWorkAsExpected);
            RunTest(() =>
            {
                // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
                // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
                // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
                // created before our consumer tried to connect.
                var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
                var topicProducer = client.GetTopicProducer(topic);
                
                // Clean previous run
                var topicStateManager = topicConsumer.GetStateManager();
                topicStateManager.DeleteStreamStates();
                topicStateManager.GetStreamStates().Should().BeEmpty();

                var msgCounter = 0;
                topicConsumer.OnStreamReceived += (sender, stream) =>
                {
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
                SpinWait.SpinUntil(() => msgCounter == 7, TimeSpan.FromSeconds(10));
                output.WriteLine($"Waited for messages, got {msgCounter}");


                msgCounter.Should().Be(7);
                output.WriteLine($"Got expected number of messages");
                topicConsumer.Commit();
                topicConsumer.Dispose();

                output.WriteLine($"Checking if topic state manager returns the expected stream states");
                var manager = App.GetStateManager().GetTopicStateManager(topic);
                manager.GetStreamStates().Should().BeEquivalentTo(new List<string>() { "stream1", "stream2" });
                
                output.WriteLine($"Checking Stream 1 Rolling sum for params");
                var stream1StateRollingSum = manager.GetStreamStateManager(streamProducer.StreamId).GetScalarState<double>("RollingSumTotal");
                var stream1StateRollingSumPerParam = manager.GetStreamStateManager(streamProducer.StreamId).GetDictionaryState<double>("RollingSum");
                stream1StateRollingSumPerParam["param1"].Should().Be(14);
                stream1StateRollingSumPerParam["param2"].Should().Be(10);
                stream1StateRollingSum.Value.Should().Be(24);
                output.WriteLine($"Checked Stream 1 Rolling sum for params");
                output.WriteLine($"Checking Stream 2 Rolling sum for params");
                var stream2StateRollingSum = manager.GetStreamStateManager(streamProducer2.StreamId).GetScalarState<double>("RollingSumTotal");
                var stream2StateRollingSumPerParam = manager.GetStreamStateManager(streamProducer2.StreamId).GetDictionaryState<double>("RollingSum");
                stream2StateRollingSumPerParam["param1"].Should().Be(9);
                stream2StateRollingSumPerParam["param2"].Should().Be(10);
                stream2StateRollingSum.Value.Should().Be(19);
                output.WriteLine($"Checked Stream 2 Rolling sum for params");

                //topicStateManager.DeleteStreamStates().Should().Be(2);
            });
        }
        
        
        [Fact]
        public void StreamState_CommittedFromAnotherThread_ShouldWorkAsExpected()
        {
            var topic = nameof(StreamState_CommittedFromAnotherThread_ShouldWorkAsExpected);
            // using Earliest as auto offset reset, because if the topic doesn't exist (should be as we're using docker for integration test with new topic)
            // using latest (the default) auto offset reset would assign us partitions after they were written, making us miss all messages.
            // therefore earliest is the best option, as the moment the partitions are created, we start reading from earliest messages, even though they were
            // created before our consumer tried to connect.
            var topicConsumer = client.GetTopicConsumer(topic, "somerandomgroup", autoOffset: AutoOffsetReset.Earliest);
            var topicProducer = client.GetTopicProducer(topic);

            var testLength = TimeSpan.FromSeconds(10);
            // Clean previous run
            var topicStateManager = topicConsumer.GetStateManager();
            topicStateManager.DeleteStreamStates();
            topicStateManager.GetStreamStates().Should().BeEmpty();

            var mre = new ManualResetEvent(false);

            var exceptionOccurred = false;

            var msgCounter = 0;
            topicConsumer.OnStreamReceived += (sender, stream) =>
            {
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

            var senderTask = Task.Run(BackgroundSender);
            var committerTask = Task.Run(BackgroundCommitter);

            Task.WaitAll(senderTask, committerTask);
            
            streamProducer.Timeseries.Flush();
            this.output.WriteLine($"Wrote {iteration} iteration");
            exceptionOccurred.Should().BeFalse();
        }

        private async Task RunTest(Func<Task> test)
        {
            var count = 0;
            while (count < MaxTestRetry)
            {
                try
                {
                    count++;
                    await test();
                    return; // success
                }
                catch (Exception ex)
                {
                    this.output.WriteLine($"Attempt {count} failed");
                    this.output.WriteLine(ex.ToString());
                }
            }
        }
        
        private void RunTest(Action test)
        {
            var count = 0;
            while (count < MaxTestRetry)
            {
                try
                {
                    count++;
                    test();
                    return; // success
                }
                catch (Exception ex)
                {
                    this.output.WriteLine($"Attempt {count} failed");
                    this.output.WriteLine(ex.ToString());
                    if (count == MaxTestRetry) throw;
                }
            }
        }

    }
}