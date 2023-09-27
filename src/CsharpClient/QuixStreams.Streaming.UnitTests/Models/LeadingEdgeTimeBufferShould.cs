using System;
using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using Quix.TestBase.Extensions;
using QuixStreams;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class LeadingEdgeTimeBufferShould
    {
        public LeadingEdgeTimeBufferShould(ITestOutputHelper helper)
        {
            QuixStreams.Logging.Factory = helper.CreateLoggerFactory();
        }
        
        [Fact]
        public void AddRowToBuffer_WithHighLeadingEdgeDelay_ShouldPublishDataCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);

            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(5000);
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));


            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs);
            }
            
            buffer.Publish();
            
            publishedData.Count.Should().Be(1);
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250, 1500, 1750, 2000, 2500));
        }

        [Fact]
        public void AddRowToBuffer_WithUnorderedTimestamps_ShouldPublishDataCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);

            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(1000);
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1250, 1000, 1300, 2000, 1400, 1750, 2500, 3000, 3250, 3000, 3300, 3500, 4500})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs); 
            }
            
            buffer.Publish();
            
            // Assert
            publishedData.Count.Should().Be(1); 
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250, 1300, 1400, 1750, 2000, 2500, 3000, 3250, 3300, 3500));
        
        }
        
        [Fact]
        public void AddRowToBuffer_WithTimestampsArrivingTooLate_ShouldRaiseOnBackfillCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);

            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(1000);
            
            var backfilledData = new List<TimeseriesData>();
            buffer.OnBackfill += (sender, data) =>
            {
                backfilledData.Add(data);
            };

            var releasedData = new List<TimeseriesData>();
            buffer.OnPublish += (sender, data) =>
            {
                releasedData.Add(data);
            };
        
            //Act
            foreach (var timestampInMs in new []{40, 1040, 60, 70})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs); 
            }
            
            buffer.Publish();
            
            foreach (var timestampInMs in new []{2040, 10, 30, 20, 40, 50})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs); 
            }
            
            buffer.Publish();
            
        
            // Assert
            releasedData.Count.Should().Be(2);
            releasedData[0].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(40));
            releasedData[1].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(50, 60, 70, 1040));
            
            backfilledData.Count.Should().Be(1);
            backfilledData[0].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(10, 20, 30, 40));
        }
        
        [Fact]
        public void AddRowToBuffer_WithTags_ShouldRaiseExpected()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);

            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(1000);
            
            var backfilledData = new List<TimeseriesData>();
            buffer.OnBackfill += (sender, data) =>
            {
                backfilledData.Add(data);
            };

            var releasedData = new List<TimeseriesData>();
            buffer.OnPublish += (sender, data) =>
            {
                releasedData.Add(data);
            };
            
            
            //Act
            // First value and tag should preserve
            buffer.GetOrCreateTimestamp(40 * (long)1e6)
                .AddValue("ms_value", 123)
                .AddTag("tag_1", "value_1"); 

            buffer.GetOrCreateTimestamp(40 * (long)1e6)
                .AddValue("ms_value", 456)
                .AddTag("tag_1", "value_2");

            // new value and tag should happen
            buffer.GetOrCreateTimestamp(50 * (long)1e6)
                .AddValue("ms_value", 123)
                .AddTag("tag_1", "value_1"); 

            buffer.GetOrCreateTimestamp(50 * (long)1e6)
                .AddValue("ms_value", 456, true)
                .AddTag("tag_1", "value_2", true);
            
            
            buffer.GetOrCreateTimestamp(5000 * (long)1e6).AddValue("ms_value", 123);

            buffer.Publish();
            
        
            // Assert
            releasedData.Count.Should().Be(1);
            var releasedAsRaw = releasedData[0].ConvertToTimeseriesDataRaw();
            releasedAsRaw.Should().BeEquivalentTo(new TimeseriesDataRaw()
            {
                Epoch = 0,
                BinaryValues = new Dictionary<string, byte[][]>(),
                StringValues = new Dictionary<string, string[]>(),
                Timestamps = new long[] {40* (long)1e6, 50* (long)1e6},
                NumericValues = new Dictionary<string, double?[]>()
                {
                    {"ms_value", new double?[] {123, 456}}
                },
                TagValues = new Dictionary<string, string[]>()
                {
                    {"tag_1", new [] {"value_1", "value_2"}}
                }
            });
        }
        
        
        [Fact]
        public void Flush_WithDataInTheBuffer_ShouldFlushEverything()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(1000);
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs);
            }
        
            buffer.Flush();
            
            // Assert
            publishedData.Count.Should().Be(1); 
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250, 1500, 1750, 2000, 2500, 9000));
        }
        
        
        [Fact]
        public void Flush_WithEpochInProducer_ShouldFlushExpected()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            var offsetTs = TimeSpan.FromSeconds(1);
            streamProducer.Epoch.Returns(DateTime.UnixEpoch + offsetTs);
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(1000);
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs + offsetTs.TotalMilliseconds);
            }
        
            buffer.Flush();
            
            // Assert
            publishedData.Count.Should().Be(1); 
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000, 2250, 2500, 2750, 3000, 3500, 10000));
        }
        
        [Fact]
        public void Flush_WithEpochInLeadingEdgeTimeBuffer_ShouldFlushExpected()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            var offsetTs = TimeSpan.FromSeconds(1);
            streamProducer.Epoch.Returns(DateTime.UnixEpoch.AddTicks(offsetTs.ToNanoseconds() * 10000 / 1000)); // this should be totally ignored
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeTimeBuffer(1000);
            buffer.Epoch = (int)(offsetTs.TotalMilliseconds * 1e6); // this should not be ignored
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs + offsetTs.TotalMilliseconds);
            }
        
            buffer.Flush();
            
            // Assert
            publishedData.Count.Should().Be(1);
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000, 2250, 2500, 2750, 3000, 3500, 10000));
        }

        [Fact]
        public void AddValue_WithDoubleParameter_OverwriteFalse_ShouldNotOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeRow(1, true, new Dictionary<string, string>());
            row.AddValue("param", 1.0);

            // Act
            row.AddValue("param", 2.0, false);

            // Assert
            row.NumericValues["param"].Should().Be(1.0);
        }

        [Fact]
        public void AddValue_WithDoubleParameter_OverwriteTrue_ShouldOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeRow(1, true, new Dictionary<string, string>());
            row.AddValue("param", 1.0);

            // Act
            row.AddValue("param", 2.0, true);

            // Assert
            row.NumericValues["param"].Should().Be(2.0);
        }
        
        [Fact]
        public void AddValue_WithStringParameter_OverwriteFalse_ShouldNotOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeRow(1, true, new Dictionary<string, string>());
            row.AddValue("param", "oldValue");

            // Act
            row.AddValue("param", "newValue", false);

            // Assert
            row.StringValues["param"].Should().Be("oldValue");
        }
        
        [Fact]
        public void AddValue_WithStringParameter_OverwriteTrue_ShouldOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeRow(1, true, new Dictionary<string, string>());
            row.AddValue("param", "oldValue");

            // Act
            row.AddValue("param", "newValue", true);

            // Assert
            row.StringValues["param"].Should().Be("newValue");
        }

        [Fact]
        public void AddValue_WithByteArrayParameter_OverwriteFalse_ShouldNotOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeRow(1, true, new Dictionary<string, string>());
            row.AddValue("param", new byte[] { 1, 2, 3 });

            // Act
            row.AddValue("param", new byte[] { 4, 5, 6 }, false);

            // Assert
            row.BinaryValues["param"].Should().Equal(new byte[] { 1, 2, 3 });
        }

        [Fact]
        public void AddValue_WithByteArrayParameter_OverwriteTrue_ShouldOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeRow(1, true, new Dictionary<string, string>());
            row.AddValue("param", new byte[] { 1, 2, 3 });

            // Act
            row.AddValue("param", new byte[] { 4, 5, 6 }, true);

            // Assert
            row.BinaryValues["param"].Should().Equal(new byte[] { 4, 5, 6 });
        }
        
        [Fact]
        public void AppendToTimeseriesData_ShouldAddAllValuesToTimeseriesData()
        {
            // Arrange
            var row = new LeadingEdgeRow(1,true, new Dictionary<string, string> { { "tag", "value" } });
            row.AddValue("param1", 1.0);
            row.AddValue("param2", "string value");
            row.AddValue("param3", new byte[] { 1, 2, 3 });

            // Act
            var result = row.AppendToTimeseriesData(new TimeseriesData());
            
            // Assert
            result.rawData.NumericValues["param1"].Should().Contain(1.0);
            result.rawData.StringValues["param2"].Should().Contain("string value");
            result.rawData.BinaryValues["param3"].Should().ContainEquivalentOf(new byte[] { 1, 2, 3 });
            result.rawData.TagValues["tag"].Should().Contain("value");
            
        }
        
        private TimeseriesDataRaw CreateTSDataRawWithFixedTimestamp(params int[] msWithEpochArray)
        {
            var data = new TimeseriesData(capacity: msWithEpochArray.Length);
            foreach (var msWithEpoch in msWithEpochArray)
            {
                data.AddTimestampNanoseconds(msWithEpoch * (long)1e6, epochIncluded: true)
                    .AddValue("ms_value", msWithEpoch);
            }

            return data.ConvertToTimeseriesDataRaw();
        }

    }
}
