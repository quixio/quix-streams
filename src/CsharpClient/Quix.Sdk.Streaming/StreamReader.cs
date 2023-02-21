using System;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming.Models.StreamReader;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Handles reading data for the assigned stream from the protocol.
    /// </summary>
    internal class StreamReader : StreamProcess, IStreamReaderInternal
    {
        private readonly IInputTopic topic;
        private readonly ILogger logger = Logging.CreateLogger<StreamReader>();
        private readonly StreamPropertiesReader streamPropertiesReader;
        private readonly StreamParametersReader streamParametersReader;
        private readonly StreamEventsReader streamEventsReader;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamReader"/>
        /// This constructor is called internally by the Stream Process factory.
        /// </summary>
        /// <param name="topic">The topic the reader belongs to</param>
        /// <param name="streamId">Stream Id of the source that has generated this Stream Process. 
        /// Commonly the Stream Id will be coming from the protocol. 
        /// If no stream Id is passed, like when a new stream is created for producing data, a Guid is generated automatically.</param>
        internal StreamReader(IInputTopic topic, string streamId): base(streamId)
        {
            this.topic = topic;
            // Managed readers
            this.streamPropertiesReader = new StreamPropertiesReader(this.topic, this);
            this.streamParametersReader = new StreamParametersReader(this.topic, this);
            this.streamEventsReader = new StreamEventsReader(this.topic, this);

            InitializeStreaming();
        }

        /// <summary>
        /// Exists for mocking purposes
        /// </summary>
        protected StreamReader()
        {
            
        }

        /// <inheritdoc />
        public StreamPropertiesReader Properties => streamPropertiesReader;

        /// <inheritdoc />
        public StreamParametersReader Parameters => streamParametersReader;

        /// <inheritdoc />
        public StreamEventsReader Events => streamEventsReader;

        /// <inheritdoc />
        public event EventHandler<PackageReceivedEventArgs> OnPackageReceived;

        /// <inheritdoc />
        public event EventHandler<StreamClosedEventArgs> OnStreamClosed;


        /// <inheritdoc />
        public virtual event Action<IStreamReader, Process.Models.StreamProperties> OnStreamPropertiesChanged;

        /// <inheritdoc />
        public virtual event Action<IStreamReader, Process.Models.ParameterDefinitions> OnParameterDefinitionsChanged;

        /// <inheritdoc />
        public virtual event Action<IStreamReader, Process.Models.TimeseriesDataRaw> OnTimeseriesData;

        /// <inheritdoc />
        public virtual  event Action<IStreamReader, Process.Models.EventDataRaw> OnEventData;

        /// <inheritdoc />
        public virtual  event Action<IStreamReader, Process.Models.EventDefinitions> OnEventDefinitionsChanged;

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

        private void OnStreamPackageReceived(IStreamProcess streamProcess, Sdk.Process.Models.StreamPackage package)
        {
            this.logger.LogTrace("StreamReader: OnPackageReceived");
            this.OnPackageReceived?.Invoke(this, new PackageReceivedEventArgs(this.topic, this, package));
        }

        private void OnStreamPropertiesReceived(IStreamProcess streamProcess, Process.Models.StreamProperties obj)
        {
            this.logger.LogTrace("StreamReader: OnStreamPropertiesReceived");
            this.OnStreamPropertiesChanged?.Invoke(this, obj);
        }

        private void OnTimeseriesDataReceived(IStreamProcess streamProcess, Process.Models.TimeseriesDataRaw obj)
        {
            this.logger.LogTrace("StreamReader: OnTimeseriesDataReceived. Data packet of size = {0}", obj.Timestamps.Length);
            this.OnTimeseriesData?.Invoke(this, obj);
        }

        private void OnParameterDefinitionsReceived(IStreamProcess streamProcess, Process.Models.ParameterDefinitions obj)
        {
            this.logger.LogTrace("StreamReader: OnParameterDefinitionsReceived");
            this.OnParameterDefinitionsChanged?.Invoke(this, obj);
        }

        private void OnEventDataReceived(IStreamProcess streamProcess, Process.Models.EventDataRaw[] events)
        {
            this.logger.LogTrace("StreamReader: OnEventDataReceived");
            for (var index = 0; index < events.Length; index++)
            {
                var ev = events[index];
                this.OnEventData?.Invoke(this, ev);
            }
        }

        private void OnEventDefinitionsReceived(IStreamProcess streamProcess, Process.Models.EventDefinitions obj)
        {
            this.logger.LogTrace("StreamReader: OnEventDefinitionsReceived");
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
            this.logger.LogTrace("StreamReader: OnStreamEndReceived");

            this.Parameters.Buffers.ForEach(buffer => buffer.Dispose());
            
            this.OnStreamClosed?.Invoke(this, new StreamClosedEventArgs(this.topic, this, endType));
        }

        public override void Dispose()
        {
            this.streamEventsReader.Dispose();
            this.streamParametersReader.Dispose();
            this.streamPropertiesReader.Dispose();
            base.Dispose();
        }
    }
}
