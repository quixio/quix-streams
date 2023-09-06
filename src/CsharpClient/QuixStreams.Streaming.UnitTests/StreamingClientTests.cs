using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Quix.TestBase.Extensions;
using QuixStreams;
using QuixStreams.Streaming.UnitTests.Helpers;
using QuixStreams.Telemetry.Models;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.UnitTests
{
    public class StreamingClientTests
    {
        public StreamingClientTests(ITestOutputHelper helper)
        {
            QuixStreams.Logging.Factory = helper.CreateLoggerFactory();
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
                BinaryValues = binaryVals,
                TagValues = new Dictionary<string, string[]>()
            };
        }

        [Theory]
        [InlineData(CodecType.Json)]
        [InlineData(CodecType.CompactJsonForBetterPerformance)]
        public void StreamingClient_Writing_ShouldReadExpectedResults(CodecType writerCodec)
        {
            
            var client = new TestStreamingClient(writerCodec);
            
            var topicConsumer = client.GetTopicConsumer();
            var topicProducer = client.GetTopicProducer();

            IList<TimeseriesDataRaw> data = new List<TimeseriesDataRaw>();
            var streamStarted = 0;
            var streamEnded = 0;
            StreamProperties streamProperties = null;
            var parametersPropertiesChanged = false;
            string streamId = null;

            topicConsumer.OnStreamReceived += (s, e) =>
            {
                if (e.StreamId != streamId)
                {
                    return;
                }

                streamStarted++;
                (e as IStreamConsumerInternal).OnTimeseriesData += (s2, e2) => data.Add(e2);
                (e as IStreamConsumerInternal).OnParameterDefinitionsChanged += (s2, e2) => parametersPropertiesChanged = true;
                (e as IStreamConsumerInternal).OnStreamPropertiesChanged += (s2, e2) => streamProperties = e2;

                e.OnStreamClosed += (s2, e2) => streamEnded++;
            };

            topicConsumer.Subscribe();


            using (var stream = topicProducer.CreateStream())
            {
                streamId = stream.StreamId;

                stream.Properties.Name = "Volvo car telemetry";
                stream.Properties.Location = "Car telemetry/Vehicles/Volvo";
                stream.Properties.AddParent("1234");
                stream.Properties.Metadata["test_key"] = "test_value";
                stream.Properties.Flush();
                SpinWait.SpinUntil(() => streamStarted == 1);
                streamStarted.Should().Be(1);
                streamProperties.Should().NotBeNull();
                streamProperties.Location.Should().Be("Car telemetry/Vehicles/Volvo");
                streamProperties.Metadata["test_key"].Should().Be("test_value");
                streamProperties.Parents.First().Should().Be("1234");

                var expectedParametersProperties = new ParameterDefinitions
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

                (stream as IStreamProducerInternal).Publish(expectedParametersProperties);

                SpinWait.SpinUntil(() => parametersPropertiesChanged, TimeSpan.FromSeconds(10));
                Assert.True(parametersPropertiesChanged, "Parameter properties event not reached reader.");

                var expectedData = new List<TimeseriesDataRaw>();
                expectedData.Add(GenerateTimeseriesData(0));
                expectedData.Add(GenerateTimeseriesData(10));

                (stream as IStreamProducerInternal).Publish(expectedData);

                SpinWait.SpinUntil(() => data.Count == 2, TimeSpan.FromSeconds(5));

                data.Should().BeEquivalentTo(expectedData);

                stream.Close();

                Assert.Equal(1, streamEnded);
            }
        }
    }
}