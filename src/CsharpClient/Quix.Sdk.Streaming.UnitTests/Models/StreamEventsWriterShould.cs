﻿using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Process.Models.Utility;
using NSubstitute;
using Xunit;

namespace Quix.Sdk.Streaming.UnitTests.Models
{
    public class StreamEventsWriterShould
    {

        [Fact]
        public void Events_AddValue_ShouldWriteExpected()
        {
            // Arrange
            var streamWriter = Substitute.For<IStreamWriterInternal>();
            var sentData = new List<EventDataRaw>();
            streamWriter.Write(Arg.Do<EventDataRaw>(x=> sentData.Add(x)));
            streamWriter.Write(Arg.Do<ICollection<EventDataRaw>>(x => sentData.AddRange(x.ToList())));

            var eventsWriter = new Streaming.Models.StreamWriter.StreamEventsWriter(streamWriter);
            var epoch = new DateTime(2000, 01, 01);
            eventsWriter.Epoch = epoch;

            // Act
            eventsWriter.AddTimestampNanoseconds(100)
                .AddValue("test_param1", "1")
                .AddValue("test_param2", "2")
                .Write();
            eventsWriter.AddTimestampNanoseconds(200)
                .AddValue("test_param2", "3")
                .Write();
            eventsWriter.AddTimestampNanoseconds(300)
                .AddValue("test_param1", "4")
                .Write();

            // Assert
            sentData.Count.Should().Be(4);
            sentData[0].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 100 + epoch.ToUnixNanoseconds(),
                Id = "test_param1",
                Value = "1",
                Tags = new Dictionary<string, string>()
            });
            sentData[1].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 100 + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
            });
            sentData[2].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 200 + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "3",
                Tags = new Dictionary<string, string>()
            });
            sentData[3].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 300 + epoch.ToUnixNanoseconds(),
                Id = "test_param1",
                Value = "4",
                Tags = new Dictionary<string, string>()
            });
        }

        [Fact]
        public void Events_AddValuesWithTags_ShouldWriteExpected()
        {
            // Arrange
            var streamWriter = Substitute.For<IStreamWriterInternal>();
            var sentData = new List<EventDataRaw>();
            streamWriter.Write(Arg.Do<EventDataRaw>(x => sentData.Add(x)));
            streamWriter.Write(Arg.Do<ICollection<EventDataRaw>>(x => sentData.AddRange(x.ToList())));

            var eventsWriter = new Streaming.Models.StreamWriter.StreamEventsWriter(streamWriter);
            var epoch = new DateTime(2000, 01, 01);
            eventsWriter.Epoch = epoch;

            // Act
            eventsWriter.AddTimestampNanoseconds(100)
                .AddValue("test_param1", "1")
                .AddValue("test_param2", "2")
                .AddTag("tag1", "value1")
                .Write();

            eventsWriter.DefaultTags["default1"] = "value1";
            eventsWriter.DefaultTags["default2"] = "value2";

            eventsWriter.AddTimestampNanoseconds(200)
                .AddValue("test_param2", "3")
                .AddTag("tag1", "value1")
                .Write();
            eventsWriter.AddTimestampNanoseconds(300)
                .AddValue("test_param1", "4")
                .Write();

            // Assert
            sentData.Count.Should().Be(4);
            sentData[0].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 100 + epoch.ToUnixNanoseconds(),
                Id = "test_param1",
                Value = "1",
                Tags = new Dictionary<string, string>()
                {
                    {  "tag1", "value1" }
                }
            });
            sentData[1].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 100 + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
                {
                    {  "tag1", "value1" },
                }
            });
            sentData[2].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 200 + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "3",
                Tags = new Dictionary<string, string>()
                {
                    {  "tag1", "value1" },
                    {  "default1", "value1" },
                    {  "default2", "value2" },
                }
            });
            sentData[3].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 300 + epoch.ToUnixNanoseconds(),
                Id = "test_param1",
                Value = "4",
                Tags = new Dictionary<string, string>()
                {
                    {  "default1", "value1" },
                    {  "default2", "value2" },
                }
            });
        }

        [Fact]
        public void Events_AddValueWithMixedTimestamps_ShouldWriteExpected()
        {
            // Arrange
            var streamWriter = Substitute.For<IStreamWriterInternal>();
            var sentData = new List<EventDataRaw>();
            streamWriter.Write(Arg.Do<EventDataRaw>(x => sentData.Add(x)));
            streamWriter.Write(Arg.Do<ICollection<EventDataRaw>>(x => sentData.AddRange(x.ToList())));

            var eventsWriter = new Streaming.Models.StreamWriter.StreamEventsWriter(streamWriter);
            var epoch = new DateTime(2000, 01, 01);
            eventsWriter.Epoch = epoch;

            // Act
            eventsWriter.AddTimestamp(new DateTime(1999, 01, 01))
                .AddValue("test_param1", "1")
                .AddValue("test_param2", "2")
                .Write();
            eventsWriter.AddTimestamp(new TimeSpan(01, 02, 03))
                .AddValue("test_param2", "2")
                .Write();
            eventsWriter.AddTimestampNanoseconds(300)
                .AddValue("test_param3", "3")
                .Write();

            // Assert
            sentData.Count.Should().Be(4);
            sentData[0].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new DateTime(1999, 01, 01).ToUnixNanoseconds(),
                Id = "test_param1",
                Value = "1",
                Tags = new Dictionary<string, string>()
            });
            sentData[1].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new DateTime(1999, 01, 01).ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
            });
            sentData[2].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new TimeSpan(01, 02, 03).ToNanoseconds() + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
            });
            sentData[3].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 300 + epoch.ToUnixNanoseconds(),
                Id = "test_param3",
                Value = "3",
                Tags = new Dictionary<string, string>()
            });
        }

        [Fact]
        public void Events_WriteDirectWithEventDataInstancesAndDefaultEpoch_ShouldWriteExpected()
        {
            // Arrange
            var streamWriter = Substitute.For<IStreamWriterInternal>();
            var sentData = new List<EventDataRaw>();
            streamWriter.Write(Arg.Do<EventDataRaw>(x => sentData.Add(x)));
            streamWriter.Write(Arg.Do<ICollection<EventDataRaw>>(x => sentData.AddRange(x.ToList())));

            var eventsWriter = new Streaming.Models.StreamWriter.StreamEventsWriter(streamWriter);
            var epoch = new DateTime(2000, 01, 01);
            eventsWriter.Epoch = epoch;

            // Act
            var data1 = new Streaming.Models.EventData( "test_param1", new DateTime(1999, 01, 01), "1");
            var data2 = new Streaming.Models.EventData("test_param2", new DateTime(1999, 01, 01), "2");
            eventsWriter.Write(new Streaming.Models.EventData[] { data1, data2 });

            eventsWriter.DefaultTags["default1"] = "value1";
            eventsWriter.DefaultTags["default2"] = "value2";

            var data3 = new Streaming.Models.EventData("test_param2", new TimeSpan(01, 02, 03), "2").AddTag("extraTag", "value1");
            eventsWriter.Write(data3);

            var data4 = new Streaming.Models.EventData("test_param3", 300, "3");
            eventsWriter.Write(data4);

            // Assert
            sentData.Count.Should().Be(4);
            sentData[0].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new DateTime(1999, 01, 01).ToUnixNanoseconds(),
                Id = "test_param1",
                Value = "1",
                Tags = new Dictionary<string, string>()
            });
            sentData[1].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new DateTime(1999, 01, 01).ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
            });
            sentData[2].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new TimeSpan(01, 02, 03).ToNanoseconds() + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
                {
                    {  "extraTag", "value1" },
                    {  "default1", "value1" },
                    {  "default2", "value2" },
                }
            });
            sentData[3].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = 300 + epoch.ToUnixNanoseconds(),
                Id = "test_param3",
                Value = "3",
                Tags = new Dictionary<string, string>()
                {
                    {  "default1", "value1" },
                    {  "default2", "value2" },
                }
            });
        }

        [Fact]
        public void Events_WriteTwice_ShouldUpdateTimestampsWithEpochOnFirstWrite()
        {
            // Arrange
            var streamWriter = Substitute.For<IStreamWriterInternal>();
            var sentData = new List<EventDataRaw>();
            streamWriter.Write(Arg.Do<EventDataRaw>(x => sentData.Add(x)));
            streamWriter.Write(Arg.Do<ICollection<EventDataRaw>>(x => sentData.AddRange(x.ToList())));

            var eventsWriter = new Streaming.Models.StreamWriter.StreamEventsWriter(streamWriter);
            var epoch = new DateTime(2000, 01, 01);
            eventsWriter.Epoch = epoch;

            // Act
            var data1 = new Streaming.Models.EventData("test_param2", new TimeSpan(01, 02, 03), "2").AddTag("extraTag", "value1");

            eventsWriter.Write(data1);
            eventsWriter.Write(data1);

            // Assert
            sentData.Count.Should().Be(2);
            sentData[0].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new TimeSpan(01, 02, 03).ToNanoseconds() + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
                {
                    {  "extraTag", "value1" },
                }
            });
            sentData[1].Should().BeEquivalentTo(new EventDataRaw
            {
                Timestamp = new TimeSpan(01, 02, 03).ToNanoseconds() + epoch.ToUnixNanoseconds(),
                Id = "test_param2",
                Value = "2",
                Tags = new Dictionary<string, string>()
                {
                    {  "extraTag", "value1" },
                }
            });
        }

        [Fact]
        public void AddDefinition_WithLocation_ShouldProduceExpectedDefinitions()
        {
            // Arrange
            var streamWriter = Substitute.For<IStreamWriterInternal>();
            var sentDefinitions = new List<EventDefinitions>();
            streamWriter.Write(Arg.Do<EventDefinitions>(x => sentDefinitions.Add(x)));
            var eventsWriter = new Streaming.Models.StreamWriter.StreamEventsWriter(streamWriter);

            // Act
            eventsWriter.DefaultLocation = ""; // root
            eventsWriter.AddDefinition("event1", "Event One", "The event one").SetLevel(EventLevel.Warning).SetCustomProperties("custom prop");
            eventsWriter.AddLocation("/some/nested/group").AddDefinition("event2");
            eventsWriter.AddLocation("some/nested/group").AddDefinition("event3");
            eventsWriter.AddLocation("some/nested/group/").AddDefinition("event4");
            eventsWriter.AddLocation("some/nested/group2/")
                .AddDefinition("event5")
                .AddDefinition("event6");
            eventsWriter.AddLocation("some/nested/group2/startswithtest") // there was an issue with this going under /some/nested/group/ also
                .AddDefinition("event7");
            eventsWriter.Flush();

            // Assert

            sentDefinitions.Count().Should().Be(1);
            var definitions = sentDefinitions[0];

            definitions.Should().BeEquivalentTo(new EventDefinitions
            {
                Events = new List<EventDefinition>()
                {
                    new EventDefinition
                    {
                        Id = "event1",
                        Name = "Event One",
                        Description = "The event one",
                        Level = EventLevel.Warning,
                        CustomProperties = "custom prop"
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
            });
        }
    }
}
