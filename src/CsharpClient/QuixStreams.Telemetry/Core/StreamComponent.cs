using System;
using System.Threading;

namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Implements one Component in a Stream pipeline chain. All components of a stream pipeline derive from this class.
    /// Each component is connected to one another using Input and Output connection points. 
    /// These connection points are used internally in the derived components to read, write and transform messages in a pipeline.
    /// </summary>
    public class StreamComponent : IStreamComponent
    {
        private IStreamPipeline streamPipeline;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamComponent"/>
        /// </summary>
        public StreamComponent()
        {
            this.Input = new IOComponentConnection();
            this.Output = new IOComponentConnection();
        }

        /// <summary>
        /// Cancellation token of the Stream pipeline
        /// </summary>
        public CancellationToken CancellationToken { get; set; } = default;

        /// <inheritdoc />
        public IStreamPipeline StreamPipeline
        {
            get => streamPipeline;
            set
            {
                if (streamPipeline != value)
                {
                    streamPipeline = value;
                    this.OnStreamPipelineAssigned?.Invoke();
                }
            }
        }

        /// <summary>
        /// Raised when a Stream pipeline is assigned to the component
        /// </summary>
        public Action OnStreamPipelineAssigned = null;


        /// <inheritdoc />
        public IIOComponentConnection Input { get; }

        /// <inheritdoc />
        public IIOComponentConnection Output { get; }
    }
}