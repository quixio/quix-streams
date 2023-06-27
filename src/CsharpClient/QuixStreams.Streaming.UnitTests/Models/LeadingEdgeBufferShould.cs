using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
        public void WriteToBuffer_WithHighLeadingEdgeDelay_ShouldPublishDataCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = new LeadingEdgeBuffer(timeseriesProducer, 5000, 500);
            
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
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Commit();
            }
            
            onDataReleasedRaiseCount.Should().Be(3);
            publishedData.Count.Should().Be(3); // (1000, 1250), (1500, 1750), 2000
            publishedData[0].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1000, 1250));
            publishedData[1].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(1500, 1750));
            publishedData[2].Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(2000));
        }

        [Fact]
        public void WriteToBuffer_WithUnorderedTimestamps_ShouldPublishDataCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = new LeadingEdgeBuffer(timeseriesProducer, 1000, 500);
            
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
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Commit(); 
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
        public void WriteToBuffer_WithTimestampsArrivingTooLate_ShouldRaiseOnBackfillCorrectly()
        {
            // Arrange
            var topicProducer = Substitute.For<ITopicProducer>();
            var streamProducer = Substitute.For<IStreamProducerInternal>();
            
            var timeseriesProducer = new QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer(topicProducer, streamProducer);

            var buffer = new LeadingEdgeBuffer(timeseriesProducer, 1000, 500);
            
            var publishedData = new List<TimeseriesDataRaw>();
            var onDataReleasedRaiseCount = 0;
            streamProducer.Publish(Arg.Do<TimeseriesDataRaw>(x=>
            {
                onDataReleasedRaiseCount++;
                publishedData.Add(x);
            }));
        
            var backfilledData = new List<TimeseriesData>();
            var onBackfillRaiseCount = 0;
            buffer.OnBackfill += (sender, args) =>
            {
                backfilledData.Add(args.Data);
                onBackfillRaiseCount++;
            };
        
            //Act
            foreach (var timestampInMs in new []{1000, 1250, 1500, 2500, 10, 20, 30})
            {
                buffer.GetOrCreateTimestamp(timestampInMs * (long)1e6).AddValue("ms_value", timestampInMs).Commit(); 
            }
            
        
            // Assert
            onBackfillRaiseCount.Should().Be(3);
            backfilledData.Count.Should().Be(3); // 10, 20, 30
            backfilledData[0].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(10));
            backfilledData[1].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(20));
            backfilledData[2].ConvertToTimeseriesDataRaw().Should().BeEquivalentTo(CreateTSDataRawWithFixedTimestamp(30));
        }
        
        
        // [Theory]
        // [InlineData(true)]
        // [InlineData(false)]
        // public void FlushData_WithLEDelayAndTimeSpanConfig_ShouldFlushCorrectly(bool initialConfig)
        // {
        //     // Arrange
        //     var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();
        //
        //     if (initialConfig)
        //     {
        //         bufferConfiguration.LeadingEdgeDelay = 5000;
        //         bufferConfiguration.TimeSpanInMilliseconds = 500;
        //     }
        //
        //     var buffer = new TimeseriesBuffer(bufferConfiguration);
        //     if (!initialConfig)
        //     {
        //         buffer.LeadingEdgeDelay = 5000;
        //         buffer.TimeSpanInMilliseconds = 500;
        //     }
        //
        //
        //     var receivedData = new List<TimeseriesData>();
        //     var onDataReleasedRaiseCount = 0;
        //     buffer.OnDataReleased += (sender, args) =>
        //     {
        //         receivedData.Add(args.Data);
        //         onDataReleasedRaiseCount++;
        //     };
        //
        //
        //     //Act
        //     foreach (var timeseriesDataWithSingleTimestamp in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000}.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
        //     {
        //         buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
        //     }
        //
        //     buffer.FlushData(false, includeDataInLeadingEdgeDelay: true);
        //
        //     // or
        //     onDataReleasedRaiseCount.Should().Be(4);
        //     receivedData.Count.Should().Be(4); // (1000, 1250), (1500, 1750), 2000, (2500, 9000)
        //     receivedData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1000, 1250));
        //     receivedData[1].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1500, 1750));
        //     receivedData[2].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(2000));
        //     receivedData[3].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(2500, 9000));
        //
        // }
        
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
