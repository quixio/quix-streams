using System;
using System.IO;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamReader;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Handles reading data for the assigned stream from the protocol.
    /// </summary>
    internal class StreamReader : StreamProcess, IStreamReader, IStreamReaderInternal
    {
        private readonly ILogger logger = Logging.CreateLogger<StreamReader>();
        private readonly StreamPropertiesReader streamPropertiesReader;
        private readonly StreamParametersReader streamParametersReader;
        private readonly StreamEventsReader streamEventsReader;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamReader"/>
        /// This constructor is called internally by the Stream Process factory.
        /// </summary>
        /// <param name="streamId">Stream Id of the source that has generated this Stream Process. 
        /// Commonly the Stream Id will be coming from the protocol. 
        /// If no stream Id is passed, like when a new stream is created for producing data, a Guid is generated automatically.</param>
        public StreamReader(string streamId): base(streamId)
        {
            // Managed readers
            this.streamPropertiesReader = new StreamPropertiesReader(this);
            this.streamParametersReader = new StreamParametersReader(this);
            this.streamEventsReader = new StreamEventsReader(this);

            InitializeStreaming();
        }

        /// <inheritdoc />
        public StreamPropertiesReader Properties => streamPropertiesReader;

        /// <inheritdoc />
        public StreamParametersReader Parameters => streamParametersReader;

        /// <inheritdoc />
        public StreamEventsReader Events => streamEventsReader;

        /// <inheritdoc />
        public event EventHandler<StreamPackage> OnPackageReceived;

        /// <inheritdoc />
        public event EventHandler<StreamEndType> OnStreamClosed;


        /// <inheritdoc />
        public event Action<IStreamReaderInternal, Process.Models.StreamProperties> OnStreamPropertiesChanged;

        /// <inheritdoc />
        public event Action<IStreamReaderInternal, Process.Models.ParameterDefinitions> OnParameterDefinitionsChanged;

        /// <inheritdoc />
        public event Action<IStreamReaderInternal, Process.Models.ParameterDataRaw> OnParameterData;

        /// <inheritdoc />
        public event Action<IStreamReaderInternal, Process.Models.EventDataRaw> OnEventData;

        /// <inheritdoc />
        public event Action<IStreamReaderInternal, Process.Models.EventDefinitions> OnEventDefinitionsChanged;

        private void InitializeStreaming()
        {
            // Modifiers
            // this.AddComponent(SimpleModifier)

            this.Subscribe<Process.Models.StreamProperties>(OnStreamPropertiesReceived);
            this.Subscribe<Process.Models.ParameterDataRaw>(OnParameterDataReceived);
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
            this.OnPackageReceived?.Invoke(this, package);
        }

        private void OnStreamPropertiesReceived(IStreamProcess streamProcess, Process.Models.StreamProperties obj)
        {
            this.logger.LogTrace("StreamReader: OnStreamPropertiesReceived");
            this.OnStreamPropertiesChanged?.Invoke(this, obj);
        }

        private void OnParameterDataReceived(IStreamProcess streamProcess, Process.Models.ParameterDataRaw obj)
        {
            this.logger.LogTrace("StreamReader: OnParameterDataReceived. Data packet of size = {0}", obj.Timestamps.Length);
            this.OnParameterData?.Invoke(this, obj);
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
            
            this.OnStreamClosed?.Invoke(this, endType);
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
