using System;
using Microsoft.Extensions.Logging;
using Quix.Streams.Process;
using Quix.Streams.Process.Models;
using Quix.Streams.Streaming.Models.StreamConsumer;

namespace Quix.Streams.Streaming
{
    /// <summary>
    /// Handles reading data for the assigned stream from the protocol.
    /// </summary>
    internal class StreamConsumer : StreamProcess, IStreamConsumerInternal
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly ILogger logger = Logging.CreateLogger<StreamConsumer>();
        private readonly StreamPropertiesConsumer streamPropertiesConsumer;
        private readonly StreamTimeseriesConsumer streamTimeseriesConsumer;
        private readonly StreamEventsConsumer streamEventsConsumer;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamConsumer"/>
        /// This constructor is called internally by the Stream Process factory.
        /// </summary>
        /// <param name="topicConsumer">The topic the reader belongs to</param>
        /// <param name="streamId">Stream Id of the source that has generated this Stream Process. 
        /// Commonly the Stream Id will be coming from the protocol. 
        /// If no stream Id is passed, like when a new stream is created for producing data, a Guid is generated automatically.</param>
        internal StreamConsumer(ITopicConsumer topicConsumer, string streamId): base(streamId)
        {
            this.topicConsumer = topicConsumer;
            // Managed readers
            this.streamPropertiesConsumer = new StreamPropertiesConsumer(this.topicConsumer, this);
            this.streamTimeseriesConsumer = new StreamTimeseriesConsumer(this.topicConsumer, this);
            this.streamEventsConsumer = new StreamEventsConsumer(this.topicConsumer, this);

            InitializeStreaming();
        }

        /// <summary>
        /// Exists for mocking purposes
        /// </summary>
        protected StreamConsumer()
        {
            
        }

        /// <inheritdoc />
        public StreamPropertiesConsumer Properties => streamPropertiesConsumer;

        /// <inheritdoc />
        public StreamTimeseriesConsumer Timeseries => streamTimeseriesConsumer;

        /// <inheritdoc />
        public StreamEventsConsumer Events => streamEventsConsumer;

        /// <inheritdoc />
        public event EventHandler<PackageReceivedEventArgs> OnPackageReceived;

        /// <inheritdoc />
        public event EventHandler<StreamClosedEventArgs> OnStreamClosed;


        /// <inheritdoc />
        public virtual event Action<IStreamConsumer, Process.Models.StreamProperties> OnStreamPropertiesChanged;

        /// <inheritdoc />
        public virtual event Action<IStreamConsumer, Process.Models.ParameterDefinitions> OnParameterDefinitionsChanged;

        /// <inheritdoc />
        public virtual event Action<IStreamConsumer, Process.Models.TimeseriesDataRaw> OnTimeseriesData;

        /// <inheritdoc />
        public virtual  event Action<IStreamConsumer, Process.Models.EventDataRaw> OnEventData;

        /// <inheritdoc />
        public virtual  event Action<IStreamConsumer, Process.Models.EventDefinitions> OnEventDefinitionsChanged;

        private void InitializeStreaming()
        {
            // Modifiers
            // this.AddComponent(SimpleModifier)

            this.Subscribe<Process.Models.StreamProperties>(OnStreamPropertiesReceived);
            this.Subscribe<Process.Models.TimeseriesDataRaw>(OnTimeseriesDataReceived);
            this.Subscribe<Process.Models.ParameterDefinitions>(OnParameterDefinitionsReceived);
            this.Subscribe<Process.Models.EventDataRaw[]>(OnEventDataReceived);
            this.Subscribe<Process.Models.EventDefinitions>(OnEventDefinitionsReceived);
            this.Subscribe<Process.Models.StreamEnd>(OnStreamEndReceived);
            this.Subscribe(OnStreamPackageReceived);

            this.OnClosed += () =>
            {
                RaiseStreamClosed(StreamEndType.Terminated);
            };
        }

        private void OnStreamPackageReceived(IStreamProcess streamProcess, Quix.Streams.Process.Models.StreamPackage package)
        {
            this.logger.LogTrace("StreamConsumer: OnPackageReceived");
            this.OnPackageReceived?.Invoke(this, new PackageReceivedEventArgs(this.topicConsumer, this, package));
        }

        private void OnStreamPropertiesReceived(IStreamProcess streamProcess, Process.Models.StreamProperties obj)
        {
            this.logger.LogTrace("StreamConsumer: OnStreamPropertiesReceived");
            this.OnStreamPropertiesChanged?.Invoke(this, obj);
        }

        private void OnTimeseriesDataReceived(IStreamProcess streamProcess, Process.Models.TimeseriesDataRaw obj)
        {
            this.logger.LogTrace("StreamConsumer: OnTimeseriesDataReceived. Data packet of size = {0}", obj.Timestamps.Length);
            this.OnTimeseriesData?.Invoke(this, obj);
        }

        private void OnParameterDefinitionsReceived(IStreamProcess streamProcess, Process.Models.ParameterDefinitions obj)
        {
            this.logger.LogTrace("StreamConsumer: OnParameterDefinitionsReceived");
            this.OnParameterDefinitionsChanged?.Invoke(this, obj);
        }

        private void OnEventDataReceived(IStreamProcess streamProcess, Process.Models.EventDataRaw[] events)
        {
            this.logger.LogTrace("StreamConsumer: OnEventDataReceived");
            for (var index = 0; index < events.Length; index++)
            {
                var ev = events[index];
                this.OnEventData?.Invoke(this, ev);
            }
        }

        private void OnEventDefinitionsReceived(IStreamProcess streamProcess, Process.Models.EventDefinitions obj)
        {
            this.logger.LogTrace("StreamConsumer: OnEventDefinitionsReceived");
            this.OnEventDefinitionsChanged?.Invoke(this, obj);
        }

        private void OnStreamEndReceived(IStreamProcess streamProcess, Process.Models.StreamEnd obj)
        {
            RaiseStreamClosed(obj.StreamEndType);
        }

        private void RaiseStreamClosed(StreamEndType endType)
        {
            if (isClosed) return;
            isClosed = true;
            this.logger.LogTrace("StreamConsumer: OnStreamEndReceived");

            this.Timeseries.Buffers.ForEach(buffer => buffer.Dispose());
            
            this.OnStreamClosed?.Invoke(this, new StreamClosedEventArgs(this.topicConsumer, this, endType));
        }

        public override void Dispose()
        {
            this.streamEventsConsumer.Dispose();
            this.streamTimeseriesConsumer.Dispose();
            this.streamPropertiesConsumer.Dispose();
            base.Dispose();
        }
    }
}
