using System;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Describes the configuration for timeseries buffers
    /// </summary>
    public class TimeseriesBufferConfiguration
    {
        /// <summary>
        /// Gets or sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
        /// of data the <see cref="TimeseriesBuffer.OnDataReleased"/> event is invoked and the data is cleared from the buffer.
        /// Defaults to null (disabled).
        /// </summary>
        public int? PacketSize { get; set; } = null;

        /// <summary>
        /// Gets or sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
        /// earliest and latest buffered timestamp surpasses this number the <see cref="TimeseriesBuffer.OnDataReleased"/> event
        /// is invoked and the data is cleared from the buffer.
        /// Defaults to None (disabled).
        /// </summary>
        public long? TimeSpanInNanoseconds { get; set; } = null;

        /// <summary>
        /// Gets or sets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
        /// earliest and latest buffered timestamp surpasses this number the <see cref="TimeseriesBuffer.OnDataReleased"/> event
        /// is invoked and the data is cleared from the buffer.
        /// Defaults to null (disabled).
        /// Note: This is a millisecond converter on top of <see cref="TimeSpanInNanoseconds"/>. They both work with same underlying value.
        /// </summary>
        public long? TimeSpanInMilliseconds
        {
            get => TimeSpanInNanoseconds /(long)1e6;
            set => this.TimeSpanInNanoseconds = value * (long)1e6;
        }


        /// <summary>
        /// Gets or set the custom function which is invoked before adding the timestamp to the buffer. If returns true, <see cref="TimeseriesBuffer.OnDataReleased"/> is invoked before adding the timestamp to it.
        /// Defaults to null (disabled).
        /// </summary>
        public Func<TimeseriesDataTimestamp, bool> CustomTriggerBeforeEnqueue { get; set; } = null;

        /// <summary>
        /// Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, <see cref="TimeseriesBuffer.OnDataReleased"/> is invoked with the entire buffer content
        /// Defaults to null (disabled).
        /// </summary>
        public Func<TimeseriesData, bool> CustomTrigger { get; set; } = null;

        /// <summary>
        /// Gets or sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not. 
        /// Defaults to null (disabled).
        /// </summary>
        public Func<TimeseriesDataTimestamp, bool> Filter { get; set; } = null;

        /// <summary>
        /// Gets or sets the maximum duration in milliseconds for which the buffer will be held before triggering <see cref="TimeseriesBuffer.OnDataReleased"/> event. 
        /// <see cref="TimeseriesBuffer.OnDataReleased"/> event is triggered when the configured <see cref="BufferTimeout"/> has elapsed from the last data received by the buffer.
        /// Defaults to null (disabled). 
        /// </summary>
        public int? BufferTimeout { get; set; } = null;
    }
}