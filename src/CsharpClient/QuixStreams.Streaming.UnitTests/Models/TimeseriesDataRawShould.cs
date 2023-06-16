using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentAssertions;
using FluentAssertions.Equivalency;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Models;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class TimeseriesDataRawShould
    {
        [Fact]
        public void SortByTimestamp_WithUnorderedTimestamps_ShouldOrderThemByAsc()
        {
            // Arrange
            var data = new TimeseriesData();
            foreach (var timestamp in new int[]{1250, 1000, 1300, 2000, 1400, 1500})
            {
                data.AddTimestampNanoseconds(timestamp, epochIncluded: true)
                    .AddValue("number", timestamp)
                    .AddValue("str", $"ts_{timestamp}")
                    .AddValue("bytes", Encoding.UTF8.GetBytes($"ts_{timestamp}"))
                    .AddTag("tag", $"ts_{timestamp}");
            }
            
            var raw = data.ConvertToTimeseriesDataRaw();

            // Act
            raw.SortByTimestamp();

            // Assert
            raw.Timestamps.Should().BeInAscendingOrder();
            raw.NumericValues["number"].Should().BeInAscendingOrder();
            raw.StringValues["str"].Should().BeInAscendingOrder();
            raw.BinaryValues["bytes"].Select(Encoding.UTF8.GetString).Should().BeInAscendingOrder();
            raw.TagValues["tag"].Should().BeInAscendingOrder();
        }
    }
}
