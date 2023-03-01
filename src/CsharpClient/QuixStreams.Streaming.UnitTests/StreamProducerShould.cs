using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Telemetry.Common.Test;
using QuixStreams.Telemetry.Kafka;
using QuixStreams.Telemetry.Models;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.Process
{
    public class StreamProducerShould
    {
        private void CreateRequirements(out string streamId, out TestTelemetryKafkaProducer testTelemetryKafkaProducer, out StreamProducer streamProducer)
        {
            streamId = "TestStream";
            var ftestKafkaProducer = new TestTelemetryKafkaProducer(new TestBroker(), streamId);
            testTelemetryKafkaProducer = ftestKafkaProducer;
            Func<string, TelemetryKafkaProducer> func = (string streamId) => ftestKafkaProducer;
            streamProducer = new StreamProducer(Substitute.For<ITopicProducerInternal>(), func, streamId);
        }
        
        [Fact]
        public void PublishEventDefinitions_WithValidValues_ShouldSendToKafkaProducer()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaProducer, out var streamProducer);

            var parameterDefinitions = new ParameterDefinitions
            {
                ParameterGroups = new List<ParameterGroupDefinition>
                {
                    new ParameterGroupDefinition()
                    {
                        Name = "1",
                        Parameters = new List<ParameterDefinition>()
                        {
                            new ParameterDefinition
                            {
                                Id = "pid1"
                            }
                        },
                        ChildGroups = new List<ParameterGroupDefinition>
                        {
                            new ParameterGroupDefinition()
                            {
                                Name = "canbedupeasdiffpath",
                            }
                        }
                    },
                    new ParameterGroupDefinition()
                    {
                        Name = "2",
                        Parameters = new List<ParameterDefinition>()
                        {
                            new ParameterDefinition
                            {
                                Id = "pid2"
                            }
                        },
                        ChildGroups = new List<ParameterGroupDefinition>
                        {
                            new ParameterGroupDefinition()
                            {
                                Name = "canbedupeasdiffpath",
                            }
                        }
                    }
                },
                Parameters = new List<ParameterDefinition>()
                {
                    new ParameterDefinition
                    {
                        Id = "pid3"
                    },
                    new ParameterDefinition
                    {
                        Id = "pid4"
                    }
                }
            };

            var ed = new EventDefinitions
            {
                Events = new List<EventDefinition>()
                {
                    new EventDefinition
                    {
                        Id = "evid3",
                        Level = EventLevel.Debug,
                    },
                    new EventDefinition
                    {
                        Id = "evid4",
                        Level = EventLevel.Error
                    }
                },
                EventGroups = new List<EventGroupDefinition>
                {
                    new EventGroupDefinition()
                    {
                        Name = "1",
                        ChildGroups = new List<EventGroupDefinition>
                        {
                            new EventGroupDefinition()
                            {
                                Name = "canbedupeasdiffpath",
                            }
                        },
                        Events = new List<EventDefinition>()
                        {
                            new EventDefinition
                            {
                                Id = "evid1",
                                Level = EventLevel.Critical
                            }
                        }
                    },
                    new EventGroupDefinition()
                    {
                        Name = "2",
                        ChildGroups = new List<EventGroupDefinition>
                        {
                            new EventGroupDefinition()
                            {
                                Name = "canbedupeasdiffpath",
                            }
                        },
                        Events = new List<EventDefinition>()
                        {
                            new EventDefinition
                            {
                                Id = "evid2"
                            }
                        }
                    }
                },
            };

            List<ParameterDefinitions> interceptedParameterDefinitions = new List<ParameterDefinitions>();
            kafkaProducer.Input.Intercept((ParameterDefinitions definitions) =>
            {
                interceptedParameterDefinitions.Add(definitions);
            });

            List<EventDefinitions> interceptedEventDefinitions = new List<EventDefinitions>();
            kafkaProducer.Input.Intercept((EventDefinitions definitions) =>
            {
                interceptedEventDefinitions.Add(definitions);
            });

            // Act
            streamProducer.Publish(parameterDefinitions);
            streamProducer.Publish(ed);

            // Assert
            interceptedParameterDefinitions.Count.Should().Be(1);
            interceptedParameterDefinitions[0].Should().Be(parameterDefinitions);
            interceptedEventDefinitions.Count.Should().Be(1);
            interceptedEventDefinitions[0].Should().Be(ed);
        }

        [Fact]
        public void PublishParameterDefinitions_WithDuplicateGroupName_ShouldThrowException()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaProducer, out var streamProducer);

            var tdp = new ParameterDefinitions
            {
                ParameterGroups = new List<ParameterGroupDefinition>
                {
                    new ParameterGroupDefinition()
                    {
                        Name = "1"
                    },
                    new ParameterGroupDefinition()
                    {
                        Name = "1"
                    }
                }
            };
            List<ParameterDefinitions> interceptedTdps = new List<ParameterDefinitions>();
            kafkaProducer.Input.Intercept((ParameterDefinitions interceptedTdp) =>
            {
                interceptedTdps.Add(interceptedTdp);
            });
            
            // Act
            Action action = () => streamProducer.Publish(tdp);
            
            // Assert
            action.Should().Throw<InvalidDataContractException>();
        }
        
        [Fact]
        public void PublishParameterDefinitions_WithDuplicateParameterId_ShouldThrowException()
        {
            // Arrange
            CreateRequirements(out var streamId, out var KafkaProducer, out var streamProducer);

            var tdp = new ParameterDefinitions
            {
                ParameterGroups = new List<ParameterGroupDefinition>
                {
                    new ParameterGroupDefinition()
                    {
                        Name = "1",
                        Parameters = new List<ParameterDefinition>()
                        {
                            new ParameterDefinition
                            {
                                Id = "pid1"
                            }
                        }
                    }
                },
                Parameters = new List<ParameterDefinition>()
                {
                    new ParameterDefinition
                    {
                        Id = "pid1"
                    }
                }
            };
            List<ParameterDefinitions> interceptedTdps = new List<ParameterDefinitions>();
            KafkaProducer.Input.Intercept((ParameterDefinitions interceptedTdp) =>
            {
                interceptedTdps.Add(interceptedTdp);
            });
            
            // Act
            Action action = () => streamProducer.Publish(tdp);
            
            // Assert
            action.Should().Throw<InvalidDataContractException>();
        }
        
        [Fact]
        public void PublishEventDefinitions_WithDuplicateEventId_ShouldThrowException()
        {
            // Arrange
            CreateRequirements(out var streamId, out var KafkaProducer, out var streamProducer);

            var ed = new EventDefinitions
            {
                Events = new List<EventDefinition>()
                {
                    new EventDefinition
                    {
                        Id = "evid1"
                    }
                },
                EventGroups = new List<EventGroupDefinition>
                {
                    new EventGroupDefinition()
                    {
                        Name = "1",
                        Events = new List<EventDefinition>()
                        {
                            new EventDefinition
                            {
                                Id = "evid1"
                            }
                        }
                    }
                },
            };
            List<EventDefinitions> interceptedDefinitions = new List<EventDefinitions>();
            KafkaProducer.Input.Intercept((EventDefinitions definitions) =>
            {
                interceptedDefinitions.Add(definitions);
            });
            
            // Act
            Action action = () => streamProducer.Publish(ed);
            
            // Assert
            action.Should().Throw<InvalidDataContractException>();
        }
        
        [Fact]
        public void PublishEventData_Valid_ShouldSendEvent()
        {
            // Arrange
            CreateRequirements(out var streamId, out var KafkaProducer, out var streamProducer);

            var @event = new EventDataRaw
            {
                Id = "abc",
                Tags = new Dictionary<string, string>()
                {
                    {"one", "two"}
                },
                Value = "Iamvalue",
                Timestamp = 12345
            };
            
            List<EventDataRaw[]> interceptedEvents = new List<EventDataRaw[]>();
            KafkaProducer.Input.Intercept((EventDataRaw[] events) =>
            {
                interceptedEvents.Add(events);
            });
            
            // Act
            streamProducer.Publish(@event);
            
            // Assert
            interceptedEvents.Count.Should().Be(1);
            interceptedEvents[0].Length.Should().Be(1);
            interceptedEvents[0][0].Should().Be(@event);
        }
        
        [Fact]
        public void PublishEvents_Valid_ShouldSendEvents()
        {
            // Arrange
            CreateRequirements(out var streamId, out var KafkaProducer, out var streamProducer);

            var inputEvents = new EventDataRaw[]
            {
                new EventDataRaw
                {
                    Id = "abc",
                    Tags = new Dictionary<string, string>()
                    {
                        {"one", "two"}
                    },
                    Value = "Iamvalue"
                },
                new EventDataRaw
                {
                    Id = "efg",
                    Tags = new Dictionary<string, string>()
                    {
                        {"three", "fwo"}
                    },
                    Value = "Iamvalue2"
                }
            };
            
            List<EventDataRaw[]> interceptedEvents = new List<EventDataRaw[]>();
            KafkaProducer.Input.Intercept((EventDataRaw[] events) =>
            {
                interceptedEvents.Add(events);
            });
            
            // Act
            streamProducer.Publish(inputEvents);
            
            // Assert
            interceptedEvents.Count.Should().Be(1);
            interceptedEvents[0].Should().BeEquivalentTo(inputEvents);
        }
    }
}