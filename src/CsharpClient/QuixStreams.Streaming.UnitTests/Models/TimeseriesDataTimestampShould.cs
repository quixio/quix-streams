using System.Collections.Generic;
using FluentAssertions;
using QuixStreams.Streaming.Models;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class TimeseriesDataTimestampShould
    {
        
        [Fact]
        public void AddTags_NullAndEmptyTags_ShouldNotThrowException()
        {

            // Act
            var tsdts = new TimeseriesData().AddTimestampNanoseconds(100);
            tsdts.AddTag("test4", "val4");
            tsdts.AddTag("test2", "val5");
            tsdts.AddTags(null);
            tsdts.AddTags(new Dictionary<string, string>());

            // Assert
            tsdts.Tags.Count.Should().Be(2);
            tsdts.Tags["test2"].Should().Be("val5");
            tsdts.Tags["test4"].Should().Be("val4");

        }
        
        [Fact]
        public void AddTags_WithTags_ShouldNotThrowException()
        {
            // Arrange
            var data = new TimeseriesData().AddTimestampNanoseconds(100);
            data.AddTag("test1", "val1")
            .AddTag("test2", "val2")
            .AddTag("test3", "val3");

            // Act
            var tsdts = new TimeseriesData().AddTimestampNanoseconds(100);
            tsdts.AddTag("test4", "val4");
            tsdts.AddTag("test2", "val5");
            tsdts.AddTags(data.Tags);

            // Assert
            tsdts.Tags.Count.Should().Be(4);
            tsdts.Tags["test1"].Should().Be("val1");
            tsdts.Tags["test2"].Should().Be("val2");
            tsdts.Tags["test3"].Should().Be("val3");
            tsdts.Tags["test4"].Should().Be("val4");

        }
        
        [Fact]
        public void ConvertToTimeseriesDataRaw_ReturnsWithSingleTimestamp()
        {
            // Arrange
            var timeseriesData = new TimeseriesData();
            timeseriesData.AddTimestampNanoseconds(100)
                .AddValue("string", "1")
                .AddTag("1", "tag1");    
            
            var timestampToConvert = timeseriesData.AddTimestampNanoseconds(200)
                .AddValue("string", "2")
                .AddValue("double", 2)
                .AddValue("byte", new byte[]{0x02})
                .AddTag("1", "tag1")
                .AddTag("2", "tag2");    
            
            

            // Act
            var raw = timestampToConvert.ConvertToTimeseriesDataRaw();

            // Assert
            raw.Timestamps.Length.Should().Be(1);
            raw.Timestamps[0].Should().Be(200);
            raw.StringValues.Count.Should().Be(1);
            raw.NumericValues.Count.Should().Be(1);
            raw.BinaryValues.Count.Should().Be(1);
            raw.TagValues.Count.Should().Be(2);
        }
    }
}