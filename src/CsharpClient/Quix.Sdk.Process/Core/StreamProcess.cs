using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Process
{
    /// <summary>
    /// Stands for one Stream Process with its own StreamId and State (if it exists)
    /// Each Stream Process is composed using a list of Stream Components
    /// </summary>
    public class StreamProcess : IStreamProcess, IDisposable
    {
        private readonly ILogger logger = Quix.Sdk.Logging.CreateLogger<StreamProcess>();
        

        private readonly CancellationToken cancellationToken;
        private readonly List<StreamComponent> componentsList = new List<StreamComponent>();
        private bool isClosed = false;
        
        /// <inheritdoc />
        public event Action OnClosing;

        /// <inheritdoc />
        public event Action OnClosed;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamProcess"/>
        /// </summary>
        /// <param name="streamId">Stream Id of the source that has generated this Stream Process. Commonly the Stream Id coming from the message broker or Transport layer. 
        /// If no stream Id is passed, like when we create a stream that is not coming from a message broker, a Guid is generated automatically.</param>
        /// <param name="cancellationToken">Cancellation token related to the Process</param>
        public StreamProcess(string streamId = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(streamId))
            {
                var isEmpty = streamId != null;
                streamId = Guid.NewGuid().ToString();
                if (isEmpty)
                {
                    logger.LogWarning("StreamId was set to empty string. As this is invalid, generating new streamId '{0}'.", streamId);
                }
            }
            else if (streamId.IndexOfAny(new char[] {'/', '\\'}) > -1)
            {
                throw new ArgumentOutOfRangeException(nameof(streamId), "Stream Id must not contain the following characters: /\\");
            }

            this.StreamId = streamId;
            this.cancellationToken = cancellationToken;

            // Add a header component of the stream process by default, to be able to operate with it completely from the start, enabling send messages or chaining it to other streams processes
            this.AddComponent(new StreamComponent());
        }

        /// <inheritdoc />
        public string StreamId { get; }

        /// <inheritdoc />
        public Dictionary<string, string> SourceMetadata { get; set; }

        /// <inheritdoc />
        public IStreamProcess AddComponent(StreamComponent component)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to add to a closed {nameof(StreamProcess)}");
            if (componentsList.Count > 0)
            {
                componentsList.Last().Output.LinkTo(component.Input); // Link Output of previous component to Input of added Component
            }

            // Assign parent stream process
            component.StreamProcess = this;
            component.CancellationToken = cancellationToken;

            componentsList.Add(component);

            return this;
        }

        // TODO: Implement State management class
        // public State State { get; }

        /// <inheritdoc />
        public Task Send(StreamPackage package)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to send to a closed {nameof(StreamProcess)}");
            return this.componentsList.First().Output.Send(package);
        }

        /// <inheritdoc />
        public Task Send<TModelType>(TModelType model)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to send to a closed {nameof(StreamProcess)}");
            return this.componentsList.First().Output.Send(model);
        }

        /// <inheritdoc />
        public IStreamProcess Subscribe(Func<IStreamProcess, StreamPackage, Task> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamProcess)}");
            // TODO tech debt, this won't work if component is added after subscription
            this.componentsList.Last().Output.Subscribe(package => onStreamPackage.Invoke(this, package));

            return this;
        }

        /// <inheritdoc />
        public IStreamProcess Subscribe<TModelType>(Func<IStreamProcess, TModelType, Task> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamProcess)}");
            // TODO tech debt, this won't work if component is added after subscription
            this.componentsList.Last().Output.Subscribe<TModelType>(model => onStreamPackage.Invoke(this, model));

            return this;
        }

        /// <inheritdoc />
        public IStreamProcess Subscribe(Action<IStreamProcess, StreamPackage> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamProcess)}");
            // TODO tech debt, this won't work if component is added after subscription
            this.componentsList.Last().Output.Subscribe(package => onStreamPackage.Invoke(this, package));

            return this;
        }

        /// <inheritdoc />
        public IStreamProcess Subscribe<TModelType>(Action<IStreamProcess, TModelType> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamProcess)}");
            // TODO tech debt, this won't work if component is added after subscription
            this.componentsList.Last().Output.Subscribe<TModelType>(model => onStreamPackage.Invoke(this, model));

            return this;
        }

        /// <inheritdoc />
        public void Close()
        {
            if (isClosed) return;
            this.OnClosing?.Invoke();
            isClosed = true;
            this.logger.LogTrace("StreamProcess: Closing stream components ({0}) {1}", this.componentsList.Count, this.StreamId);
            foreach (var component in this.componentsList.ToList())
            {
                this.logger.LogTrace("StreamProcess: Closing stream component ({0}) {1}", component.GetType().Name, this.StreamId);
                (component as IDisposable)?.Dispose();
            }

            this.OnClosed?.Invoke();
        }

        /// <inheritdoc />
        public virtual void Dispose()
        {
            this.Close();
        }
    }
}