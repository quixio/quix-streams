using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Telemetry.Models;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class StreamTimeseriesConsumerShould
    {
        private readonly ITestOutputHelper testOutputHelper;

        public StreamTimeseriesConsumerShould(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task AsyncIterate_ShouldIterateUntilClose()
        {
            var iteration = 0;
            var totalIteration = 10;
            while (iteration < totalIteration)
            {
                iteration++;
                const int NumberTimestampsTest = 10000;

                var streamConsumer = Substitute.For<IStreamConsumerInternal>();
                var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();
                var timeseriesConsumer =
                    new QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer(
                        new TestStreamingClient().GetTopicConsumer(), streamConsumer);

                var task = Task.Run(() =>
                {
                    //Act
                    for (var i = 1; i <= NumberTimestampsTest; i++)
                    {
                        var timeseriesData = new QuixStreams.Streaming.Models.TimeseriesData();
                        timeseriesData.AddTimestampNanoseconds(100 * i)
                            .AddValue($"test_numeric_param{i}", i)
                            .AddValue($"test_string_param{i}", $"{i}")
                            .AddTag($"tag{i}", $"{i}");

                        streamConsumer.OnTimeseriesData +=
                            Raise.Event<Action<IStreamConsumer, TimeseriesDataRaw>>(streamConsumer,
                                timeseriesData.ConvertToTimeseriesDataRaw());
                    }

                    streamConsumer.OnStreamClosed += Raise.Event<EventHandler<StreamClosedEventArgs>>(streamConsumer,
                        new StreamClosedEventArgs(null, streamConsumer, StreamEndType.Aborted));
                });

                var counter = 0;
                try
                {
                    await foreach (var row in timeseriesConsumer)
                    {
                        counter++;
                        if (counter % 50 == 0) testOutputHelper.WriteLine($"Counter: {counter}");
                    }
                }
                catch (Exception ex)
                {
                    throw;
                }

                await task;
                counter.Should().Be(NumberTimestampsTest, $"iteration {iteration} should also be {NumberTimestampsTest}");
            }
        }

        [Fact]
        public void Receive_TimeseriesData_ShouldRaiseExpectedOnReceivedEvents()
        {
            const int PacketSizeTest = 10;
            const int NumberTimestampsTest = 1000;

            // Arrange
            var streamConsumer = Substitute.For<IStreamConsumerInternal>();
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();
            var timeseriesConsumer = new QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer(new TestStreamingClient().GetTopicConsumer(), streamConsumer);

            var buffer = timeseriesConsumer.CreateBuffer();
            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };
            buffer.PacketSize = PacketSizeTest;

            //Act
            for (var i = 1; i <= NumberTimestampsTest; i++)
            {
                var timeseriesData = new QuixStreams.Streaming.Models.TimeseriesData();
                timeseriesData.AddTimestampNanoseconds(100 * i)
                    .AddValue($"test_numeric_param{i}", i)
                    .AddValue($"test_string_param{i}", $"{i}")
                    .AddTag($"tag{i}", $"{i}");

                streamConsumer.OnTimeseriesData += Raise.Event<Action<IStreamConsumer, TimeseriesDataRaw>>(streamConsumer, timeseriesData.ConvertToTimeseriesDataRaw());
            }

            // Assert
            receivedData.Count.Should().Be(NumberTimestampsTest / PacketSizeTest);

            for (var packet = 0; packet < NumberTimestampsTest / PacketSizeTest; packet++)
            {
                for (var i = 1; i <= PacketSizeTest; i++)
                {
                    receivedData[packet].Timestamps[i - 1].TimestampNanoseconds.Should().Be(100 * (i + packet * PacketSizeTest));
                    receivedData[packet].Timestamps[i - 1].Parameters[$"test_numeric_param{i + packet * PacketSizeTest}"].NumericValue.Should().Be(i + packet * PacketSizeTest);
                    receivedData[packet].Timestamps[i - 1].Parameters[$"test_string_param{i + packet * PacketSizeTest}"].StringValue.Should().Be($"{i + packet * PacketSizeTest}");
                    receivedData[packet].Timestamps[i - 1].Tags[$"tag{i + packet * PacketSizeTest}"].Should().Be($"{i + packet * PacketSizeTest}");
                }
            }
        }

        [Fact]
        public void Receive_Definitions_ShouldUpdateDefinitionsProperly()
        {
            // Arrange
            var streamConsumer = Substitute.For<IStreamConsumerInternal>();
            var timeseriesConsumer = new QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer(new TestStreamingClient().GetTopicConsumer(), streamConsumer);

            var parameterDefinitions = new ParameterDefinitions
            {
                Parameters = new List<ParameterDefinition>()
                {
                    new ParameterDefinition
                    {
                        Id = "Param1",
                        Name = "Parameter One",
                        Description = "The parameter one",
                        Format = "{0}%",
                        Unit = "%",
                        MinimumValue = -10.43,
                        MaximumValue = 100.123,
                        CustomProperties = "custom prop"
                    }
                },
                ParameterGroups = new List<ParameterGroupDefinition>()
                {
                    new ParameterGroupDefinition
                    {
                        Name = "some",
                        Parameters = new List<ParameterDefinition>(),
                        ChildGroups = new List<ParameterGroupDefinition>()
                        {
                            new ParameterGroupDefinition
                            {
                                Name = "nested",
                                Parameters = new List<ParameterDefinition>(),
                                ChildGroups = new List<ParameterGroupDefinition>()
                                {
                                    new ParameterGroupDefinition
                                    {
                                        Name = "group",
                                        Parameters = new List<ParameterDefinition>
                                        {
                                            new ParameterDefinition
                                            {
                                                Id = "param2"
                                            },
                                            new ParameterDefinition
                                            {
                                                Id = "param3"
                                            },
                                            new ParameterDefinition
                                            {
                                                Id = "param4"
                                            }
                                        },
                                        ChildGroups = new List<ParameterGroupDefinition>()
                                    },
                                    new ParameterGroupDefinition
                                    {
                                        Name = "group2",
                                        Parameters = new List<ParameterDefinition>
                                        {
                                            new ParameterDefinition
                                            {
                                                Id = "param5"
                                            },
                                            new ParameterDefinition
                                            {
                                                Id = "param6"
                                            }
                                        },
                                        ChildGroups = new List<ParameterGroupDefinition>
                                        {
                                            new ParameterGroupDefinition
                                            {
                                                Name = "startswithtest",
                                                ChildGroups = new List<ParameterGroupDefinition>(),
                                                Parameters = new List<ParameterDefinition>
                                                {
                                                    new ParameterDefinition
                                                    {
                                                        Id = "param7"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            };

            var expectedDefinitions = new List<QuixStreams.Streaming.Models.ParameterDefinition>
            {
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "Param1",
                    Name = "Parameter One",
                    Description = "The parameter one",
                    Format = "{0}%",
                    Unit = "%",
                    MinimumValue = -10.43,
                    MaximumValue = 100.123,
                    CustomProperties = "custom prop",
                    Location = ""
                },
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "param2",
                    Location = "/some/nested/group"
                },
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "param3",
                    Location = "/some/nested/group"

                },
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "param4",
                    Location = "/some/nested/group"
                },
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "param5",
                    Location = "/some/nested/group2"
                },
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "param6",
                    Location = "/some/nested/group2"
                },
                new QuixStreams.Streaming.Models.ParameterDefinition
                {
                    Id = "param7",
                    Location = "/some/nested/group2/startswithtest"
                }
            };

            // Act
            streamConsumer.OnParameterDefinitionsChanged += Raise.Event<Action<IStreamConsumer, ParameterDefinitions>>(streamConsumer, parameterDefinitions);

            // Assert
            timeseriesConsumer.Definitions.Count.Should().Be(7);
            timeseriesConsumer.Definitions.Should().BeEquivalentTo(expectedDefinitions);
        }

    }
}
