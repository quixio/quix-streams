using System;
using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using Quix.TestBase.Extensions;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Models;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class LeadingEdgeBufferShould
    {
        public LeadingEdgeBufferShould(ITestOutputHelper helper)
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

            var buffer = timeseriesProducer.CreateLeadingEdgeBuffer(5000, 500);
            
            var publishedData = new List<TimeseriesDataRaw>();
            var onDataReleasedRaiseCount = 0;
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                onDataReleasedRaiseCount++;
                publishedData.Add(x);
            }));


            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Publish();
            }
            
            onDataReleasedRaiseCount.Should().Be(3);
            publishedData.Count.Should().Be(3); // (1000, 1250), (1500, 1750), 2000
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250));
            publishedData[1].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1500, 1750));
            publishedData[2].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000));
        }

        [Fact]
        public void AddRowToBuffer_WithUnorderedTimestamps_ShouldPublishDataCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);

            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeBuffer(1000, 500);
            
            var publishedData = new List<TimeseriesDataRaw>();
            var onDataReleasedRaiseCount = 0;
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                onDataReleasedRaiseCount++;
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1250, 1000, 1300, 2000, 1400, 1750, 2500, 3000, 3250, 3000, 3300, 3500, 6000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Publish(); 
            }
            
            // Assert
            onDataReleasedRaiseCount.Should().Be(4);
            publishedData.Count.Should().Be(4); // (1000, 1250, 1300, 1400) + (1750, 2000), 2500, (3000, 3250, 3300)
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250, 1300, 1400));
            publishedData[1].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1750, 2000));
            publishedData[2].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2500));
            publishedData[3].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(3000, 3250, 3300));
        
        }
        
        [Fact]
        public void AddRowToBuffer_WithTimestampsArrivingTooLate_ShouldRaiseOnBackfillCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeBuffer(1000, 500);
            
            var backfilledData = new List<TimeseriesData>();
            var onBackfillRaiseCount = 0;
            buffer.OnBackfill += (sender, data) =>
            {
                backfilledData.Add(data);
                onBackfillRaiseCount++;
            };
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 2500, 10, 20, 30})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Publish(); 
            }
            
        
            // Assert
            onBackfillRaiseCount.Should().Be(3);
            backfilledData.Count.Should().Be(3); // 10, 20, 30
            backfilledData[0].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(10));
            backfilledData[1].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(20));
            backfilledData[2].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(30));
        }
        
        
        [Fact]
        public void Flush_WithDataInTheBuffer_ShouldFlushEverything()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            streamProducer.Epoch.Returns(DateTime.UnixEpoch);
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeBuffer(1000, 500);
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Publish(); 
            }
        
            buffer.Flush();
            
            // Assert
            publishedData.Count.Should().Be(4); // (1000, 1250), (1500, 1750), 2000, (2500, 9000)
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250));
            publishedData[1].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1500, 1750));
            publishedData[2].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000));
            publishedData[3].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2500, 9000));
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

            var buffer = timeseriesProducer.CreateLeadingEdgeBuffer(1000, 500);
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs + offsetTs.TotalMilliseconds).Publish(); 
            }
        
            buffer.Flush();
            
            // Assert
            publishedData.Count.Should().Be(4); // (2000, 2250), (2500, 2750), 3000, (3500, 10000)
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000, 2250));
            publishedData[1].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2500, 2750));
            publishedData[2].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(3000));
            publishedData[3].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(3500, 10000));
        }
        
        [Fact]
        public void Flush_WithEpochInLeadingEdgeBuffer_ShouldFlushExpected()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            var offsetTs = TimeSpan.FromSeconds(1);
            streamProducer.Epoch.Returns(DateTime.UnixEpoch + offsetTs * 10000); // this should be totally ignored
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = timeseriesProducer.CreateLeadingEdgeBuffer(1000, 500);
            buffer.Epoch = (int)(offsetTs.TotalMilliseconds * 1e6); // this should not be ignored
            
            var publishedData = new List<TimeseriesDataRaw>();
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                publishedData.Add(x);
            }));
        
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs + offsetTs.TotalMilliseconds).Publish(); 
            }
        
            buffer.Flush();
            
            // Assert
            publishedData.Count.Should().Be(4); // (2000, 2250), (2500, 2750), 3000, (3500, 10000)
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000, 2250));
            publishedData[1].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2500, 2750));
            publishedData[2].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(3000));
            publishedData[3].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(3500, 10000));
        }

        [Fact]
        public void AddValue_WithDoubleParameter_OverwriteFalse_ShouldNotOverwriteExistingValue()
        {
            // Arrange
            var row = new LeadingEdgeTimestamp(null, 1, true, new Dictionary<string, string>());
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
            var row = new LeadingEdgeTimestamp(null, 1, true, new Dictionary<string, string>());
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
            var row = new LeadingEdgeTimestamp(null, 1, true, new Dictionary<string, string>());
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
            var row = new LeadingEdgeTimestamp(null, 1, true, new Dictionary<string, string>());
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
            var row = new LeadingEdgeTimestamp(null, 1, true, new Dictionary<string, string>());
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
            var row = new LeadingEdgeTimestamp(null, 1, true, new Dictionary<string, string>());
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
            var row = new LeadingEdgeTimestamp(null, 1,true, new Dictionary<string, string> { { "tag", "value" } });
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
