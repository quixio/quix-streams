using System;
using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Streaming.UnitTests.Helpers;
using QuixStreams.Telemetry.Models;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class StreamEventsConsumerShould
    {
        [Fact]
        public void Receive_EventData_ShouldRaiseExpectedOnReceiveEvents()
        {
            const int NumberEventsTest = 1000;

            // Arrange
            var streamConsumer = Substitute.For<IStreamConsumerInternal>();
            var receivedData = new List<QuixStreams.Streaming.Models.EventData>();
            var eventsReader = new QuixStreams.Streaming.Models.StreamConsumer.StreamEventsConsumer(new TestStreamingClient().GetTopicConsumer(), streamConsumer);

            eventsReader.OnDataReceived += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            for (var i = 0; i < NumberEventsTest; i++)
            {
                var eventData = new QuixStreams.Streaming.Models.EventData($"event{i}", 100 * i, $"test_event_value{i}")
                    .AddTag($"tag{i}", $"{i}");

                streamConsumer.OnEventData += Raise.Event<Action<IStreamConsumer, EventDataRaw>>(streamConsumer, eventData.ConvertToEventDataRaw());
            }

            // Assert
            receivedData.Count.Should().Be(NumberEventsTest);

            for (var i = 0; i < NumberEventsTest; i++)
            {
                receivedData[i].TimestampNanoseconds.Should().Be(100 * i);
                receivedData[i].Id.Should().Be($"event{i}");
                receivedData[i].Value.Should().Be($"test_event_value{i}");
                receivedData[i].Tags[$"tag{i}"].Should().Be($"{i}");
            }
        }



        [Fact]
        public void Receive_Definitions_ShouldUpdateDefinitionsProperly()
        {
            // Arrange
            var streamConsumer = Substitute.For<IStreamConsumerInternal>();
            var eventsReader = new QuixStreams.Streaming.Models.StreamConsumer.StreamEventsConsumer(new TestStreamingClient().GetTopicConsumer(), streamConsumer);

            var eventDefinitions = new EventDefinitions
            {
                Events = new List<EventDefinition>()
                {
                    new EventDefinition
                    {
                        Id = "Event1",
                        Name = "Event One",
                        Description = "The event one",
                        CustomProperties = "custom prop",
                        Level = EventLevel.Critical
                    }
                },
                EventGroups = new List<EventGroupDefinition>()
                {
                    new EventGroupDefinition
                    {
                        Name = "some",
                        Events = new List<EventDefinition>(),
                        ChildGroups = new List<EventGroupDefinition>()
                        {
                            new EventGroupDefinition
                            {
                                Name = "nested",
                                Events = new List<EventDefinition>(),
                                ChildGroups = new List<EventGroupDefinition>()
                                {
                                    new EventGroupDefinition
                                    {
                                        Name = "group",
                                        Events = new List<EventDefinition>
                                        {
                                            new EventDefinition
                                            {
                                                Id = "event2"
                                            },
                                            new EventDefinition
                                            {
                                                Id = "event3"
                                            },
                                            new EventDefinition
                                            {
                                                Id = "event4"
                                            }
                                        },
                                        ChildGroups = new List<EventGroupDefinition>()
                                    },
                                    new EventGroupDefinition
                                    {
                                        Name = "group2",
                                        Events = new List<EventDefinition>
                                        {
                                            new EventDefinition
                                            {
                                                Id = "event5"
                                            },
                                            new EventDefinition
                                            {
                                                Id = "event6"
                                            }
                                        },
                                        ChildGroups = new List<EventGroupDefinition>
                                        {
                                            new EventGroupDefinition
                                            {
                                                Name = "startswithtest",
                                                ChildGroups = new List<EventGroupDefinition>(),
                                                Events = new List<EventDefinition>
                                                {
                                                    new EventDefinition
                                                    {
                                                        Id = "event7"
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

            var expectedDefinitions = new List<QuixStreams.Streaming.Models.EventDefinition>
            {
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "Event1",
                    Name = "Event One",
                    Description = "The event one",
                    CustomProperties = "custom prop",
                    Location = "",
                    Level = EventLevel.Critical
                },
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "event2",
                    Location = "/some/nested/group"
                },
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "event3",
                    Location = "/some/nested/group"

                },
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "event4",
                    Location = "/some/nested/group"
                },
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "event5",
                    Location = "/some/nested/group2"
                },
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "event6",
                    Location = "/some/nested/group2"
                },
                new QuixStreams.Streaming.Models.EventDefinition
                {
                    Id = "event7",
                    Location = "/some/nested/group2/startswithtest"
                }
            };

            // Act
            streamConsumer.OnEventDefinitionsChanged += Raise.Event<Action<IStreamConsumer, EventDefinitions>>(streamConsumer, eventDefinitions);

            // Assert
            eventsReader.Definitions.Count.Should().Be(7);
            eventsReader.Definitions.Should().BeEquivalentTo(expectedDefinitions);
        }

    }
}
