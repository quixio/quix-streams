using System;
using System.Collections.Generic;
using QuixStreams.Streaming.Models.StreamConsumer;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// A class for producing timeseries data to an <see cref="IStreamProducer"/> in a buffered manner.
    /// </summary>
    public class TimeseriesBufferProducer: TimeseriesBuffer
    {
        private readonly ITopicProducer topicProducer;
        private readonly IStreamProducerInternal streamProducer;
        private long epoch = 0;
        private bool isDisposed;

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesBufferProducer"/>
        /// </summary>
        /// <param name="topicProducer">The topic producer which owns the stream producer</param>
        /// <param name="streamProducer">The Stream producer which owns this timeseries buffer producer</param>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        internal TimeseriesBufferProducer(ITopicProducer topicProducer, IStreamProducerInternal streamProducer, TimeseriesBufferConfiguration bufferConfiguration)
            : base(bufferConfiguration, null, true, true)
        {
            this.topicProducer = topicProducer;
            this.streamProducer = streamProducer;

            this.OnRawReleased += RawReleasedDataHandler;
        }

        private void RawReleasedDataHandler(object sender, TimeseriesDataRawReadEventArgs args)
        {
            this.streamProducer.Publish(args.Data);
        }

        /// <summary>
        /// Default Epoch used for Timestamp parameter values. Datetime added on top of all the Timestamps.
        /// </summary>
        public DateTime Epoch
        {
            get
            {
                return epoch.FromUnixNanoseconds();
            }
            set
            {
                epoch = value.ToUnixNanoseconds();
            }
        }

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// Note, <see cref="Epoch"/> is not used when invoking with a <see cref="DateTime"/>
        /// </summary>
        /// <param name="dateTime">The datetime to use for adding new parameter values</param>
        /// <returns>Timeseries data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestamp(DateTime dateTime) => this.AddTimestampNanoseconds(dateTime.ToUnixNanoseconds(), 0);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeSpan">The time since the default <see cref="Epoch"/> to add the parameter values at</param>
        /// <returns>Timeseries data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestamp(TimeSpan timeSpan) => this.AddTimestampNanoseconds(timeSpan.ToNanoseconds());

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeMilliseconds">The time in milliseconds since the default <see cref="Epoch"/> to add the parameter values at</param>
        /// <returns>Timeseries data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestampMilliseconds(long timeMilliseconds) => this.AddTimestampNanoseconds(timeMilliseconds * (long)1e6);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeNanoseconds">The time in nanoseconds since the default <see cref="Epoch"/> to add the parameter values at</param>
        /// <returns>Timeseries data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestampNanoseconds(long timeNanoseconds)
        {
            return AddTimestampNanoseconds(timeNanoseconds, this.epoch);
        }

        private TimeseriesDataBuilder AddTimestampNanoseconds(long timestampNanoseconds, long epoch)
        {
            var data = new TimeseriesData();
            var timestamp = data.AddTimestampNanoseconds(timestampNanoseconds + epoch, true);

            return new TimeseriesDataBuilder(this, data, timestamp);
        }


        /// <summary>
        /// Publish timeseries data to the buffer.
        /// </summary>
        /// <param name="data">Data to publish</param>
        public void Publish(TimeseriesData data)
        {
            for(var index = 0; index < data.Timestamps.Count; index++)
            {
                var timestamp = data.Timestamps[index];

                if (!timestamp.EpochIncluded)
                {
                    timestamp.TimestampNanoseconds += this.Epoch.ToUnixNanoseconds();
                    timestamp.EpochIncluded = true;
                }

                foreach (var kv in this.DefaultTags)
                {
                    if (!timestamp.Tags.ContainsKey(kv.Key))
                    {
                        timestamp.AddTag(kv.Key, kv.Value);
                    }
                }
            }

            this.WriteChunk(data.ConvertToTimeseriesDataRaw(false, false)); // use merge & clean of Buffer is more efficient
        }
        
        /// <summary>
        /// Publish timeseries data raw to the buffer.
        /// </summary>
        /// <param name="data">Data to publish</param>
        public void Publish(TimeseriesDataRaw data)
        {
            long epochDifference = this.streamProducer.Epoch.ToUnixNanoseconds();
            long[] updatedTimestamps = null;
            
            if (epochDifference != 0)
            {
                updatedTimestamps = new long[data.Timestamps.Length];
                for (int i = 0; i < updatedTimestamps.Length; i++)
                {
                    updatedTimestamps[i] = data.Timestamps[i] + epochDifference;
                }
            }
            
            Dictionary<string, string[]> updatedTagValues = null;

            if (this.DefaultTags.Count > 0)
            {
                updatedTagValues = new Dictionary<string, string[]>(data.TagValues);
                foreach (var tag in this.DefaultTags)
                {
                    if (!updatedTagValues.TryGetValue(tag.Key, out var tagValues))
                    {
                        tagValues = new string[data.Timestamps.Length];
                        updatedTagValues[tag.Key] = tagValues;
                    }

                    for (int i = 0; i < data.Timestamps.Length; i++)
                    {
                        tagValues[i] = tag.Value;
                    }
                }
            }

            var newData = new QuixStreams.Telemetry.Models.TimeseriesDataRaw(
                data.Epoch, 
                updatedTimestamps ?? data.Timestamps, 
                data.NumericValues, 
                data.StringValues, 
                data.BinaryValues, 
                updatedTagValues ?? data.TagValues
            );

            this.WriteChunk(newData);
        }
        
        /// <summary>
        /// Publish single timestamp to the buffer.
        /// </summary>
        /// <param name="timestamp">Timeseries timestamp to publish</param>
        public void Publish(TimeseriesDataTimestamp timestamp)
        {
            if (!timestamp.EpochIncluded)
            {
                timestamp.TimestampNanoseconds += this.Epoch.ToUnixNanoseconds();
                timestamp.EpochIncluded = true;
            }

            foreach (var kv in this.DefaultTags)
            {
                if (!timestamp.Tags.ContainsKey(kv.Key))
                {
                    timestamp.AddTag(kv.Key, kv.Value);
                }
            }
            this.WriteChunk(timestamp.ConvertToTimeseriesDataRaw());
        }


        /// <summary>
        /// Default tags injected for all parameters values sent by this buffer.
        /// </summary>
        public Dictionary<string, string> DefaultTags { get; set; } = new Dictionary<string, string>();

        /// <summary>
        /// Immediately publishes the data from the buffer without waiting for the buffer condition to be fulfilled.
        /// </summary>
        public void Flush()
        {
            this.FlushData(false);
        }
        
        protected override void InvokeOnReceive(object sender, TimeseriesDataReadEventArgs args)
        {
            base.InvokeOnReceive(this, new TimeseriesDataReadEventArgs(this.topicProducer, this.streamProducer, args.Data));
        }

        protected override void InvokeOnRawReceived(object sender, TimeseriesDataRawReadEventArgs args)
        {
            base.InvokeOnRawReceived(this, new TimeseriesDataRawReadEventArgs(this.topicProducer, this.streamProducer, args.Data));
        }

        /// <summary>
        /// Flushes the internal buffers and disposes the object.
        /// </summary>
        public override void Dispose()
        {
            if (this.isDisposed) return;
            this.isDisposed = true;
            this.OnRawReleased -= RawReleasedDataHandler;
            base.Dispose();
        }

    }
}
