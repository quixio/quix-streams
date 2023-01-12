using System;
using System.Threading;

namespace Quix.Sdk.Process
{
    /// <summary>
    /// Implements one Component in a Stream process chain. All components of a stream process derive from this class.
    /// Each component is connected to one another using Input and Output connection points. 
    /// These connection points are used internally in the derived components to read, write and transform messages in a process.
    /// </summary>
    public class StreamComponent : IStreamComponent
    {
        private IStreamProcess streamProcess;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamComponent"/>
        /// </summary>
        public StreamComponent()
        {
            this.Input = new IOComponentConnection();
            this.Output = new IOComponentConnection();
        }

        /// <summary>
        /// Cancellation token of the Stream Process
        /// </summary>
        public CancellationToken CancellationToken { get; set; } = default;

        /// <inheritdoc />
        public IStreamProcess StreamProcess
        {
            get => streamProcess;
            set
            {
                if (streamProcess != value)
                {
                    streamProcess = value;
                    this.OnStreamProcessAssigned?.Invoke();
                }
            }
        }

        /// <summary>
        /// Handler raised when a Stream process is assigned to the component
        /// </summary>
        public Action OnStreamProcessAssigned = null;


        /// <inheritdoc />
        public IIOComponentConnection Input { get; }

        /// <inheritdoc />
        public IIOComponentConnection Output { get; }
    }
}