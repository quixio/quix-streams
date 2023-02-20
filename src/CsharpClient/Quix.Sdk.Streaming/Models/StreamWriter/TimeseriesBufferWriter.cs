using Quix.Sdk.Process.Models.Utility;
using System;
using System.Collections.Generic;
using Quix.Sdk.Process.Models;
using System.Linq;
using System.Text;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Class used to write time-series to <see cref="IStreamWriter"/> in a buffered manner
    /// </summary>
    public class TimeseriesBufferWriter: TimeseriesBuffer, IDisposable
    {
        private readonly IStreamWriterInternal streamWriter;
        private long epoch = 0;
        private bool isDisposed;

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesBufferWriter"/>
        /// </summary>
        /// <param name="streamWriter">Stream writer owner</param>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        internal TimeseriesBufferWriter(IStreamWriterInternal streamWriter, TimeseriesBufferConfiguration bufferConfiguration)
            : base(bufferConfiguration, null, true, true)
        {
            this.streamWriter = streamWriter;

            this.OnReadRaw += OnReadDataRaw;
        }

        private void OnReadDataRaw(object sender, TimeseriesDataRaw TimeseriesDataRaw)
        {
            this.streamWriter.Write(TimeseriesDataRaw);
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
        /// Note, <see cref="Epoch"/> is not used when invoking with <see cref="DateTime"/>
        /// </summary>
        /// <param name="dateTime">The datetime to use for adding new parameter values</param>
        /// <returns>Parameter data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestamp(DateTime dateTime) => this.AddTimestampNanoseconds(dateTime.ToUnixNanoseconds(), 0);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeSpan">The time since the default <see cref="Epoch"/> to add the parameter values at</param>
        /// <returns>Parameter data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestamp(TimeSpan timeSpan) => this.AddTimestampNanoseconds(timeSpan.ToNanoseconds());

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeMilliseconds">The time in milliseconds since the default <see cref="Epoch"/> to add the parameter values at</param>
        /// <returns>Parameter data builder to add parameter values at the provided time</returns>
        public TimeseriesDataBuilder AddTimestampMilliseconds(long timeMilliseconds) => this.AddTimestampNanoseconds(timeMilliseconds * (long)1e6);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeNanoseconds">The time in nanoseconds since the default <see cref="Epoch"/> to add the parameter values at</param>
        /// <returns>Parameter data builder to add parameter values at the provided time</returns>
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
        /// Write parameter data to the buffer
        /// </summary>
        /// <param name="data">Data to write</param>
        public void Write(TimeseriesData data)
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

            this.WriteChunk(data.ConvertToProcessData(false, false)); // use merge & clean of Buffer is more efficient
        }

        
        /// <summary>
        /// Default tags injected for all parameters values sent by this buffer.
        /// </summary>
        public Dictionary<string, string> DefaultTags { get; set; } = new Dictionary<string, string>();

        /// <summary>
        /// Immediately writes the data from the buffer without waiting for buffer condition to fulfill
        /// </summary>
        public void Flush()
        {
            this.FlushData(false);
        }

        /// <summary>
        /// Flushes internal buffers and disposes
        /// </summary>
        public override void Dispose()
        {
            if (this.isDisposed) return;
            this.isDisposed = true;
            this.OnReadRaw -= OnReadDataRaw;
            base.Dispose();
        }

    }
}
