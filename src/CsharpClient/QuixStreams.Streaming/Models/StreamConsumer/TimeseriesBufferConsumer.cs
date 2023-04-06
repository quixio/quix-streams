namespace QuixStreams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Represents a class for consuming data from a stream in a buffered manner.
    /// </summary>
    public class TimeseriesBufferConsumer: TimeseriesBuffer
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of the <see cref="TimeseriesBufferConsumer"/> class.
        /// </summary>
        /// <param name="topicConsumer">The topic consumer which owns the stream consumer.</param>
        /// <param name="streamConsumer">The Stream consumer which owns this timeseries buffer consumer.</param>
        /// <param name="bufferConfiguration">The configuration settings for the buffer.</param>
        /// <param name="parametersFilter">An array of parameters used for filtering the data in the buffer.</param>
        internal TimeseriesBufferConsumer(ITopicConsumer topicConsumer, IStreamConsumerInternal streamConsumer, TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter)
            : base(bufferConfiguration, parametersFilter, true, false)
        {
            this.topicConsumer = topicConsumer;
            this.streamConsumer = streamConsumer;

            this.streamConsumer.OnTimeseriesData += OnTimeseriesDataEventHandler;
        }

        /// <summary>
        /// Disposes the resources used by the <see cref="TimeseriesBufferConsumer"/> instance.
        /// </summary>
        public override void Dispose()
        {
            this.streamConsumer.OnTimeseriesData -= OnTimeseriesDataEventHandler;
            base.Dispose();
        }

        /// <summary>
        /// Handles the event when timeseries data is received.
        /// </summary>
        /// <param name="streamConsumer">The stream consumer associated with the event.</param>
        /// <param name="timeseriesDataRaw">Data received in TimeseriesDataRaw format .</param>
        private void OnTimeseriesDataEventHandler(IStreamConsumer streamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            this.WriteChunk(timeseriesDataRaw);
        }

        /// <summary>
        /// Invokes the OnReceive event with the received timeseries data.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="args">The arguments associated with the event, containing the received data.</param>
        protected override void InvokeOnReceive(object sender, TimeseriesDataReadEventArgs args)
        {
            base.InvokeOnReceive(this, new TimeseriesDataReadEventArgs(this.topicConsumer, this.streamConsumer, args.Data));
        }

        /// <summary>
        /// Invokes the OnRawReceived event with the received data in TimeseriesDataRaw format.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="args">The arguments associated with the event, containing the received raw data.</param>
        protected override void InvokeOnRawReceived(object sender, TimeseriesDataRawReadEventArgs args)
        {
            base.InvokeOnRawReceived(this, new TimeseriesDataRawReadEventArgs(this.topicConsumer, this.streamConsumer, args.Data));
        }
    }
}
