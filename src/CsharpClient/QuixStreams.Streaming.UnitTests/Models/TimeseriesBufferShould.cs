using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FluentAssertions;
using Quix.TestBase.Extensions;
using QuixStreams.Streaming.Models;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class TimeseriesBufferShould
    {
        public TimeseriesBufferShould(ITestOutputHelper helper)
        {
            QuixStreams.Logging.Factory = helper.CreateLoggerFactory();
        }

        [Fact]
        public void WriteData_WithDisabledConfiguration_ShouldRaiseOnReceiveEventsStraightForward()
        {
            // when the buffer is disabled it is expected that each frame passed to the buffer is raised as-is.
            
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            var buffer = new TimeseriesBuffer(bufferConfiguration);
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));

            // Assert
            receivedData.Count.Should().Be(1);
            receivedData.First().Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Take(5).ToList()));   
        }

        [Fact]
        public void WriteData_WithData_FlushOnDispose()
        {
            // Arrange
            var bufferConfiguration = new TimeseriesBufferConfiguration() // Set the buffer explicitly to null
            {
                PacketSize = 3,
                TimeSpanInMilliseconds = null,
                TimeSpanInNanoseconds = null,
                BufferTimeout = null,
                Filter = null,
                CustomTrigger = null,
                CustomTriggerBeforeEnqueue = null
            };
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));

            // Assert
            receivedData.Count.Should().Be(1);
            receivedData[0].Timestamps.Count.Should().Be(3);

            buffer.Dispose();

            receivedData.Count.Should().Be(2);
            receivedData[1].Timestamps.Count.Should().Be(2);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithPacketSizeConfiguration_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.PacketSize = 2;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.PacketSize = 2;
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));

            // Assert
            receivedData.Count.Should().Be(2);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(0).Take(2).ToList()));
            receivedData[1].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(2).Take(2).ToList()));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithTimeSpanMsConfiguration_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.TimeSpanInMilliseconds = 200;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.TimeSpanInMilliseconds = 200;
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));

            // Assert
            receivedData.Count.Should().Be(2);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(0).Take(2).ToList()));
            receivedData[1].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(2).Take(2).ToList()));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithTimeSpanNsConfiguration_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.TimeSpanInNanoseconds = 200 * (long) 1e6;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.TimeSpanInNanoseconds = 200 * (long) 1e6;
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));

            // Assert
            receivedData.Count.Should().Be(2);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(0).Take(2).ToList()));
            receivedData[1].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(2).Take(2).ToList()));
        }

        [Fact]
        public void WriteData_WithTimeSpanAndBufferTimeoutConfiguration_ShouldRaiseOnDataReleasedCorrectly()
        {
            // Arrange
            var bufferConfiguration = new TimeseriesBufferConfiguration() 
            {
                PacketSize = null,
                TimeSpanInMilliseconds = 200,
                BufferTimeout = 100,
                Filter = null,
                CustomTrigger = null,
                CustomTriggerBeforeEnqueue = null
            };
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));
            Thread.Sleep(1000);

            // Assert
            receivedData.Count.Should().Be(3);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(0).Take(2).ToList()));
            receivedData[1].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(2).Take(2).ToList()));
            receivedData[2].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(4).Take(1).ToList()));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithFilterConfiguration_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // when the buffer is disabled it is expected that each frame passed to the buffer is raised as-is.
            
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.Filter = (timestamp) => timestamp.Parameters["param2"].NumericValue == 2;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.Filter = (timestamp) => timestamp.Parameters["param2"].NumericValue == 2;
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));
            Thread.Sleep(1000);

            // Assert
            receivedData.Count.Should().Be(1);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Where((ds, i) => i % 2 == 0 ).ToList()));
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithBufferTimeout_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.BufferTimeout = 100;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.BufferTimeout = 100;
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));
            Thread.Sleep(1000);

            // Assert
            receivedData.Count.Should().Be(1);
            receivedData[0].Should().BeEquivalentTo(data);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithCustomTriggerConfiguration_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.CustomTrigger = (data) => data.Timestamps.Count == 2;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.CustomTrigger = (data) => data.Timestamps.Count == 2;
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));
            Thread.Sleep(1000);

            // Assert
            receivedData.Count.Should().Be(2);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(0).Take(2).ToList()));
            receivedData[1].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(2).Take(2).ToList()));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithCustomTriggerBeforeEnqueueConfiguration_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.CustomTriggerBeforeEnqueue = timestamp => timestamp.Tags["tag2"] == "value2";
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.CustomTriggerBeforeEnqueue = timestamp => timestamp.Tags["tag2"] == "value2";
            var receivedData = new List<QuixStreams.Streaming.Models.TimeseriesData>();

            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
            };

            //Act
            var data = this.GenerateTimeseriesData();
            buffer.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false));
            Thread.Sleep(1000);

            // Assert
            receivedData.Count.Should().Be(2);
            receivedData[0].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(0).Take(2).ToList()));
            receivedData[1].Should().BeEquivalentTo(new TimeseriesData(data.Timestamps.Skip(2).Take(2).ToList()));
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithLEDelayConfig_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig) bufferConfiguration.LeadingEdgeDelay = 500;
            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig) buffer.LeadingEdgeDelay = 500;
            
            var receivedData = new List<TimeseriesData>();
            var onDataReleasedRaiseCount = 0;
            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
                onDataReleasedRaiseCount++;
            };
            
            //Act
            foreach (var timeseriesDataWithSingleTimestamp in new []{300, 200, 400, 500, 800, 900, 1000}.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
            {
                buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
            }

            // Assert
            onDataReleasedRaiseCount.Should().Be(3);
            receivedData.Count.Should().Be(3); // (200, 300), 400, 500
            receivedData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(200, 300));
            receivedData[1].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(400));
            receivedData[2].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(500));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithLEDelayAndTimeSpanConfig_ShouldRaiseOnDataReleasedCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig)
            {
                bufferConfiguration.LeadingEdgeDelay = 5000;
                bufferConfiguration.TimeSpanInMilliseconds = 500;
            }

            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig)
            {
                buffer.LeadingEdgeDelay = 5000;
                buffer.TimeSpanInMilliseconds = 500;
            }
        

            var receivedData = new List<TimeseriesData>();
            var onDataReleasedRaiseCount = 0;
            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
                onDataReleasedRaiseCount++;
            };


            //Act
            foreach (var timeseriesDataWithSingleTimestamp in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000}.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
            {
                buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
            }
            
            onDataReleasedRaiseCount.Should().Be(3);
            receivedData.Count.Should().Be(3); // (1000, 1250), (1500, 1750), 2000
            receivedData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1000, 1250));
            receivedData[1].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1500, 1750));
            receivedData[2].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(2000));
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithLEDelayAndTimeSpanConfig_ShouldRaiseOnDataReleasedWithOrderedTimestamps(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig)
            {
                bufferConfiguration.LeadingEdgeDelay = 1000;
                bufferConfiguration.TimeSpanInMilliseconds = 500;
            }

            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig)
            {
                buffer.LeadingEdgeDelay = 1000;
                buffer.TimeSpanInMilliseconds = 500;
            }
        

            var receivedData = new List<TimeseriesData>();
            var onDataReleasedRaiseCount = 0;
            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
                onDataReleasedRaiseCount++;
            };


            //Act
            foreach (var timeseriesDataWithSingleTimestamp in new []{1250, 1000, 1300, 2000, 1400, 1750, 2500, 3000}.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
            {
                buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
            }
            
            // Sending timestamps in a single timestamp data
            var x = CreateTimeseriesDataWithFixedTimestamp(3250, 3000, 3300, 3500, 6000);
            buffer.WriteChunk(x.ConvertToTimeseriesDataRaw(false, false));
            
            // Assert
            onDataReleasedRaiseCount.Should().Be(4);
            receivedData.Count.Should().Be(4); // (1000, 1250, 1300, 1400) + (1750, 2000), 2500, (3000, 3250, 3300)
            receivedData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1000, 1250, 1300, 1400));
            receivedData[1].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1750, 2000));
            receivedData[2].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(2500));
            receivedData[3].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(3000, 3250, 3300));
            
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithLEDelayAndTimeSpanConfig_WithoutDataReleasedEvent_ShouldRaiseOnBackfillCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig)
            {
                bufferConfiguration.LeadingEdgeDelay = 1000;
                bufferConfiguration.TimeSpanInMilliseconds = 500;
            }

            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig)
            {
                buffer.LeadingEdgeDelay = 1000;
                buffer.TimeSpanInMilliseconds = 500;
            }
            
            buffer.OnDataReleased += (sender, args) => { };
            
            var backfilledData = new List<TimeseriesData>();
            var onBackfillRaiseCount = 0;
            buffer.OnBackfill += (sender, args) =>
            {
                backfilledData.Add(args.Data);
                onBackfillRaiseCount++;
            };

            //Act
            foreach (var timeseriesDataWithSingleTimestamp in new []{1000, 1250, 1500, 2500, 10, 20}.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
            {
                buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
            }

            buffer.WriteChunk(CreateTimeseriesDataWithFixedTimestamp(30, 40).ConvertToTimeseriesDataRaw(false, false));
            
            // Assert
            onBackfillRaiseCount.Should().Be(3);
            backfilledData.Count.Should().Be(3); // 10, 20, (30, 40)
            backfilledData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(10));
            backfilledData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(20));
            backfilledData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(30, 40));
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteData_WithLEDelayAndTimeSpanConfig_WithDataReleasedEvent_ShouldRaiseOnBackfillCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig)
            {
                bufferConfiguration.LeadingEdgeDelay = 1000;
                bufferConfiguration.TimeSpanInMilliseconds = 500;
            }

            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig)
            {
                buffer.LeadingEdgeDelay = 1000;
                buffer.TimeSpanInMilliseconds = 500;
            }
            
            var backfilledData = new List<TimeseriesData>();
            var onBackfillRaiseCount = 0;
            buffer.OnBackfill += (sender, args) =>
            {
                backfilledData.Add(args.Data);
                onBackfillRaiseCount++;
            };

            //Act
            foreach (var timeseriesDataWithSingleTimestamp in new []{1000, 1250, 1500, 2500, 10, 20 }.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
            {
                buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
            }

            buffer.WriteChunk(CreateTimeseriesDataWithFixedTimestamp(30, 40).ConvertToTimeseriesDataRaw(false, false));
            
            // Assert
            onBackfillRaiseCount.Should().Be(3);
            backfilledData.Count.Should().Be(3); // 10, 20, (30, 40)
            backfilledData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(10));
            backfilledData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(20));
            backfilledData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(30, 40));
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void FlushData_WithLEDelayAndTimeSpanConfig_ShouldFlushCorrectly(bool initialConfig)
        {
            // Arrange
            var bufferConfiguration = GetEmptyTimeseriesBufferConfiguration();

            if (initialConfig)
            {
                bufferConfiguration.LeadingEdgeDelay = 5000;
                bufferConfiguration.TimeSpanInMilliseconds = 500;
            }

            var buffer = new TimeseriesBuffer(bufferConfiguration);
            if (!initialConfig)
            {
                buffer.LeadingEdgeDelay = 5000;
                buffer.TimeSpanInMilliseconds = 500;
            }
        

            var receivedData = new List<TimeseriesData>();
            var onDataReleasedRaiseCount = 0;
            buffer.OnDataReleased += (sender, args) =>
            {
                receivedData.Add(args.Data);
                onDataReleasedRaiseCount++;
            };


            //Act
            foreach (var timeseriesDataWithSingleTimestamp in new []{1000, 1250, 1500, 1750, 2000, 2500, 9000}.Select(x => CreateTimeseriesDataWithFixedTimestamp(x)))
            {
                buffer.WriteChunk(timeseriesDataWithSingleTimestamp.ConvertToTimeseriesDataRaw(false, false)); 
            }
            
            buffer.FlushData(false, includeDataInLeadingEdgeDelay: true);
            
            // or
            onDataReleasedRaiseCount.Should().Be(4);
            receivedData.Count.Should().Be(4); // (1000, 1250), (1500, 1750), 2000, (2500, 9000)
            receivedData[0].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1000, 1250));
            receivedData[1].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(1500, 1750));
            receivedData[2].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(2000));
            receivedData[3].Should().BeEquivalentTo(CreateTimeseriesDataWithFixedTimestamp(2500, 9000));
            
        }

        private TimeseriesBufferConfiguration GetEmptyTimeseriesBufferConfiguration()
        {
            return new TimeseriesBufferConfiguration
            {
                PacketSize = null,
                TimeSpanInMilliseconds = null,
                TimeSpanInNanoseconds = null,
                LeadingEdgeDelay = null,
                BufferTimeout = null,
                Filter = null,
                CustomTrigger = null,
                CustomTriggerBeforeEnqueue = null
            };
        }
        
        private TimeseriesData GenerateTimeseriesData()
        {
            var data = new TimeseriesData();
            data.AddTimestampMilliseconds(100)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param3", "3")
                .AddValue("param4", "4")
                .AddTag("tag1", "value1");

            data.AddTimestampMilliseconds(200)
                .AddValue("param3", "3")
                .AddValue("param4", "4")
                .AddTag("tag1", "value1");

            data.AddTimestampMilliseconds(300)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddTag("tag2", "value2");

            data.AddTimestampMilliseconds(400)
                .AddValue("param1", 1);

            data.AddTimestampMilliseconds(500)
                .AddValue("param2", 2)
                .AddTag("tag2", "value2");

            return data;
        }

        private TimeseriesData CreateTimeseriesDataWithFixedTimestamp(params int[] msWithEpochArray)
        {
            var data = new TimeseriesData();
            foreach (var msWithEpoch in msWithEpochArray)
            {
                data.AddTimestampNanoseconds(msWithEpoch * (long)1e6, epochIncluded: true)
                    .AddValue("ms_value", msWithEpoch);
            }

            return data;
        }
    }
}
