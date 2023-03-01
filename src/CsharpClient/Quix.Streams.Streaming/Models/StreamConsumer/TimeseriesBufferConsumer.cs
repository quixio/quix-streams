namespace Quix.Streams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Class used to read from the stream in a buffered manner
    /// </summary>
    public class TimeseriesBufferConsumer: TimeseriesBuffer
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesBufferConsumer"/>
        /// </summary>
        /// <param name="topicConsumer">The topic it belongs to</param>
        /// <param name="streamConsumer">Stream reader owner</param>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        internal TimeseriesBufferConsumer(ITopicConsumer topicConsumer, IStreamConsumerInternal streamConsumer, TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter)
            : base(bufferConfiguration, parametersFilter, true, false)
        {
            this.topicConsumer = topicConsumer;
            this.streamConsumer = streamConsumer;

            this.streamConsumer.OnTimeseriesData += OnTimeseriesDataEventHandler;
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            this.streamConsumer.OnTimeseriesData -= OnTimeseriesDataEventHandler;
            base.Dispose();
        }

        private void OnTimeseriesDataEventHandler(IStreamConsumer streamConsumer, Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            this.WriteChunk(timeseriesDataRaw);
        }

        protected override void InvokeOnReceive(object sender, TimeseriesDataReadEventArgs args)
        {
            base.InvokeOnReceive(this, new TimeseriesDataReadEventArgs(this.topicConsumer, this.streamConsumer, args.Data));
        }

        protected override void InvokeOnRawReceived(object sender, TimeseriesDataRawReadEventArgs args)
        {
            base.InvokeOnRawReceived(this, new TimeseriesDataRawReadEventArgs(this.topicConsumer, this.streamConsumer, args.Data));
        }
    }
}
