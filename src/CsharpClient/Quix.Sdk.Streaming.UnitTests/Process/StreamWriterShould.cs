using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using FluentAssertions;
using Quix.Sdk.Process;
using Quix.Sdk.Process.Common.Test;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming;
using NSubstitute;
using Xunit;

namespace Quix.Sdk.Streaming.UnitTests.Process
{
    public class StreamWriterShould
    {
        private void CreateRequirements(out string streamId, out TestKafkaWriter testKafkaWriter, out StreamWriter streamWriter)
        {
            streamId = "TestStream";
            var ftestKafkaWriter = new TestKafkaWriter(new TestBroker(), streamId);
            testKafkaWriter = ftestKafkaWriter;
            Func<string, KafkaWriter> func = (string streamId) => ftestKafkaWriter;
            streamWriter = new StreamWriter(Substitute.For<IOutputTopicInternal>(), func, streamId);
        }
        
        [Fact]
        public void WriteEventDefinitions_WithValidValues_ShouldSendToKafkaWriter()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaWriter, out var streamWriter);

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
            kafkaWriter.Input.Intercept((ParameterDefinitions definitions) =>
            {
                interceptedParameterDefinitions.Add(definitions);
            });

            List<EventDefinitions> interceptedEventDefinitions = new List<EventDefinitions>();
            kafkaWriter.Input.Intercept((EventDefinitions definitions) =>
            {
                interceptedEventDefinitions.Add(definitions);
            });

            // Act
            streamWriter.Write(parameterDefinitions);
            streamWriter.Write(ed);

            // Assert
            interceptedParameterDefinitions.Count.Should().Be(1);
            interceptedParameterDefinitions[0].Should().Be(parameterDefinitions);
            interceptedEventDefinitions.Count.Should().Be(1);
            interceptedEventDefinitions[0].Should().Be(ed);
        }

        [Fact]
        public void WriteParameterDefinitions_WithDuplicateGroupName_ShouldThrowException()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaWriter, out var streamWriter);

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
            kafkaWriter.Input.Intercept((ParameterDefinitions interceptedTdp) =>
            {
                interceptedTdps.Add(interceptedTdp);
            });
            
            // Act
            Action action = () => streamWriter.Write(tdp);
            
            // Assert
            action.Should().Throw<InvalidDataContractException>();
        }
        
        [Fact]
        public void WriteParameterDefinitions_WithDuplicateParameterId_ShouldThrowException()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaWriter, out var streamWriter);

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
            kafkaWriter.Input.Intercept((ParameterDefinitions interceptedTdp) =>
            {
                interceptedTdps.Add(interceptedTdp);
            });
            
            // Act
            Action action = () => streamWriter.Write(tdp);
            
            // Assert
            action.Should().Throw<InvalidDataContractException>();
        }
        
        [Fact]
        public void WriteEventDefinitions_WithDuplicateEventId_ShouldThrowException()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaWriter, out var streamWriter);

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
            kafkaWriter.Input.Intercept((EventDefinitions definitions) =>
            {
                interceptedDefinitions.Add(definitions);
            });
            
            // Act
            Action action = () => streamWriter.Write(ed);
            
            // Assert
            action.Should().Throw<InvalidDataContractException>();
        }
        
        [Fact]
        public void WriteEventData_Valid_ShouldSendEvent()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaWriter, out var streamWriter);

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
            kafkaWriter.Input.Intercept((EventDataRaw[] events) =>
            {
                interceptedEvents.Add(events);
            });
            
            // Act
            streamWriter.Write(@event);
            
            // Assert
            interceptedEvents.Count.Should().Be(1);
            interceptedEvents[0].Length.Should().Be(1);
            interceptedEvents[0][0].Should().Be(@event);
        }
        
        [Fact]
        public void WriteEvents_Valid_ShouldSendEvents()
        {
            // Arrange
            CreateRequirements(out var streamId, out var kafkaWriter, out var streamWriter);

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
            kafkaWriter.Input.Intercept((EventDataRaw[] events) =>
            {
                interceptedEvents.Add(events);
            });
            
            // Act
            streamWriter.Write(inputEvents);
            
            // Assert
            interceptedEvents.Count.Should().Be(1);
            interceptedEvents[0].Should().BeEquivalentTo(inputEvents);
        }
    }
}