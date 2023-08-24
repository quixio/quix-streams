using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Stands for one Stream pipeline with its own StreamId and State (if it exists)
    /// Each Stream pipeline is composed using a list of Stream Components
    /// </summary>
    public class StreamPipeline : IStreamPipeline, IDisposable
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<StreamPipeline>();
        
        private readonly CancellationToken cancellationToken;
        private readonly List<StreamComponent> componentsList = new List<StreamComponent>();
        private StreamComponent firstComponent = null;
        private StreamComponent lastComponent = null;
        private bool defaultPipeline;
        private bool subscribed = false;

        private bool isClosed = false;

        /// <summary>
        /// Stream id to use when message key has not been set.
        /// </summary>
        public const string DefaultStreamIdWhenMissing = "NO_STREAM_ID";

        /// <inheritdoc />
        public event Action OnClosing;

        /// <inheritdoc />
        public event Action OnClosed;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPipeline"/>
        /// </summary>
        /// <param name="streamId">Stream Id of the source that has generated this Stream pipeline. Commonly the Stream Id coming from the message broker or Transport layer. 
        /// If no stream Id is passed, like when we create a stream that is not coming from a message broker, a Guid is generated automatically.</param>
        /// <param name="cancellationToken">Cancellation token related to the pipeline</param>
        public StreamPipeline(string streamId = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(streamId))
            {
                var isEmpty = streamId != null;
                streamId = Guid.NewGuid().ToString();
                if (isEmpty)
                {
                    streamId = DefaultStreamIdWhenMissing;
                }
            }
            else if (streamId.IndexOfAny(new char[] {'/', '\\'}) > -1)
            {
                throw new ArgumentOutOfRangeException(nameof(streamId), "Stream Id must not contain the following characters: /\\");
            }

            this.StreamId = streamId;
            this.cancellationToken = cancellationToken;

            // Add a header component of the stream pipeline by default, to be able to operate with
            // it completely from the start, enabling sending messages or chaining it to other streams pipelines
            this.firstComponent = new StreamComponent();
            this.firstComponent.Input.LinkTo(this.firstComponent.Output);
            this.lastComponent = this.firstComponent;
            this.firstComponent.StreamPipeline = this;
            this.firstComponent.CancellationToken = cancellationToken;
            this.defaultPipeline = true;
            this.AddComponent(this.firstComponent);
        }

        /// <inheritdoc />
        public string StreamId { get; }

        /// <inheritdoc />
        public Dictionary<string, string> SourceMetadata { get; set; }

        /// <inheritdoc />
        public IStreamPipeline AddComponent(StreamComponent component)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to add to a closed {nameof(StreamPipeline)}");
            if (subscribed)  throw new InvalidOperationException($"Unable to add to a pipeline {nameof(StreamPipeline)} that is already subscribed to");

            if (this.defaultPipeline)
            {
                componentsList.Clear();
                this.firstComponent = component;
            }
            else
            {
                lastComponent.Output.LinkTo(component.Input); // Link Output of previous component to Input of added Component
            }
            lastComponent = component;

            // Assign parent stream pipeline
            component.StreamPipeline = this;
            component.CancellationToken = cancellationToken;

            componentsList.Add(component);

            return this;
        }

        /// <inheritdoc />
        public Task Send(StreamPackage package)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to send to a closed {nameof(StreamPipeline)}");
            return this.firstComponent.Input.Send(package);
        }

        /// <inheritdoc />
        public Task Send<TModelType>(TModelType model)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to send to a closed {nameof(StreamPipeline)}");
            return this.firstComponent.Input.Send(model);
        }

        /// <inheritdoc />
        public IStreamPipeline Subscribe(Func<IStreamPipeline, StreamPackage, Task> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamPipeline)}");
            this.lastComponent.Output.Subscribe(package => onStreamPackage.Invoke(this, package));
            subscribed = true;
            
            return this;
        }

        /// <inheritdoc />
        public IStreamPipeline Subscribe<TModelType>(Func<IStreamPipeline, TModelType, Task> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamPipeline)}");
            this.lastComponent.Output.Subscribe<TModelType>(model => onStreamPackage.Invoke(this, model));
            subscribed = true;

            return this;
        }

        /// <inheritdoc />
        public IStreamPipeline Subscribe(Action<IStreamPipeline, StreamPackage> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamPipeline)}");
            this.lastComponent.Output.Subscribe(package => onStreamPackage.Invoke(this, package));
            subscribed = true;

            return this;
        }

        /// <inheritdoc />
        public IStreamPipeline Subscribe<TModelType>(Action<IStreamPipeline, TModelType> onStreamPackage)
        {
            if (isClosed) throw new InvalidOperationException($"Unable to subscribe to a closed {nameof(StreamPipeline)}");
            this.lastComponent.Output.Subscribe<TModelType>(model => onStreamPackage.Invoke(this, model));
            subscribed = true;

            return this;
        }

        /// <inheritdoc />
        public void Close()
        {
            if (isClosed) return;
            this.OnClosing?.Invoke();
            isClosed = true;
            this.logger.LogTrace("StreamPipeline: Closing stream components ({0}) {1}", this.componentsList.Count, this.StreamId);
            foreach (var component in this.componentsList.ToList())
            {
                this.logger.LogTrace("StreamPipeline: Closing stream component ({0}) {1}", component.GetType().Name, this.StreamId);
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