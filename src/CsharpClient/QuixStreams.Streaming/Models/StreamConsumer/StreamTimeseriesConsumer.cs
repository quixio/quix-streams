using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Helper class for reader <see cref="ParameterDefinitions"/> and <see cref="TimeseriesData"/>
    /// </summary>
    public class StreamTimeseriesConsumer : IDisposable, IAsyncEnumerable<TimeseriesDataTimestamp>
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamTimeseriesConsumer"/>
        /// </summary>
        /// <param name="topicConsumer">The topic the stream to what this reader belongs to</param>
        /// <param name="streamConsumer">Stream reader owner</param>
        internal StreamTimeseriesConsumer(ITopicConsumer topicConsumer, IStreamConsumerInternal streamConsumer)
        {
            this.topicConsumer = topicConsumer;
            this.streamConsumer = streamConsumer;

            this.streamConsumer.OnParameterDefinitionsChanged += OnParameterDefinitionsChangedEventHandler;

            this.streamConsumer.OnTimeseriesData += OnTimeseriesDataEventHandler;
            this.streamConsumer.OnTimeseriesData += OnTimeseriesDataRawEventHandler;
        }

        private void OnParameterDefinitionsChangedEventHandler(IStreamConsumer sender, ParameterDefinitions parameterDefinitions)
        {
            this.LoadFromTelemetryDefinitions(parameterDefinitions);

            this.OnDefinitionsChanged?.Invoke(this.streamConsumer, new ParameterDefinitionsChangedEventArgs(this.topicConsumer, this.streamConsumer));
        }

        /// <summary>
        /// Create a new Parameters buffer for reading data
        /// </summary>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <returns>Parameters reading buffer</returns>
        public TimeseriesBufferConsumer CreateBuffer(TimeseriesBufferConfiguration bufferConfiguration = null, params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferConsumer(this.topicConsumer, this.streamConsumer, bufferConfiguration, parametersFilter);
            this.Buffers.Add(buffer);

            return buffer;
        }

        /// <summary>
        /// Create a new Parameters buffer for reading data
        /// </summary>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <returns>Parameters reading buffer</returns>
        public TimeseriesBufferConsumer CreateBuffer(params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferConsumer(this.topicConsumer, this.streamConsumer, null, parametersFilter);
            this.Buffers.Add(buffer);

            return buffer;
        }

        /// <summary>
        /// Raised when the parameter definitions have changed for the stream.
        /// See <see cref="Definitions"/> for the latest set of parameter definitions
        /// </summary>
        public event EventHandler<ParameterDefinitionsChangedEventArgs> OnDefinitionsChanged;

        /// <summary>
        /// Event raised when data is available to read (without buffering)
        /// This event does not use Buffers and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesDataReadEventArgs> OnDataReceived;

        /// <summary>
        /// Event raised when data is received (without buffering) in raw transport format
        /// This event does not use Buffers and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesDataRawReadEventArgs> OnRawReceived;

        /// <summary>
        /// Gets the latest set of event definitions
        /// </summary>
        public List<ParameterDefinition> Definitions { get; private set; } = new List<ParameterDefinition>();

        /// <summary>
        /// List of buffers created for this stream
        /// </summary>
        internal List<TimeseriesBufferConsumer> Buffers { get; private set; } = new List<TimeseriesBufferConsumer>();

        private void LoadFromTelemetryDefinitions(QuixStreams.Telemetry.Models.ParameterDefinitions definitions)
        {
            var defs = new List<ParameterDefinition>();
            
            if (definitions.Parameters != null) 
                this.ConvertParameterDefinitions(definitions.Parameters, "").ForEach(d => defs.Add(d));
            if (definitions.ParameterGroups != null)
                this.ConvertGroupParameterDefinitions(definitions.ParameterGroups, "").ForEach(d => defs.Add(d));

            this.Definitions = defs;
        }

        private List<ParameterDefinition> ConvertParameterDefinitions(List<QuixStreams.Telemetry.Models.ParameterDefinition> parameterDefinitions, string location)
        {
            var result = parameterDefinitions.Select(d => new ParameterDefinition
            {
                Id = d.Id,
                Name = d.Name,
                Description = d.Description,
                MinimumValue = d.MinimumValue,
                MaximumValue = d.MaximumValue,
                Unit = d.Unit,
                Format = d.Format,
                CustomProperties = d.CustomProperties,
                Location = location
            }).ToList();

            return result;
        }

        private List<ParameterDefinition> ConvertGroupParameterDefinitions(List<QuixStreams.Telemetry.Models.ParameterGroupDefinition> parameterGroupDefinitions, string location)
        {
            var result = new List<ParameterDefinition>();

            foreach (var group in parameterGroupDefinitions)
            {
                if (group.Parameters != null)
                    this.ConvertParameterDefinitions(group.Parameters, location + "/" + group.Name).ForEach(d => result.Add(d));
                if (group.ChildGroups != null)
                    this.ConvertGroupParameterDefinitions(group.ChildGroups, location + "/" + group.Name).ForEach(d => result.Add(d));
            }

            return result;
        }

        private void OnTimeseriesDataEventHandler(IStreamConsumer streamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            if (this.OnDataReceived == null) return;
            var tsdata = new TimeseriesData(timeseriesDataRaw, null, false, false);
            this.OnDataReceived?.Invoke(streamConsumer, new TimeseriesDataReadEventArgs(this.topicConsumer, streamConsumer, tsdata));
        }

        private void OnTimeseriesDataRawEventHandler(IStreamConsumer streamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            this.OnRawReceived?.Invoke(streamConsumer, new TimeseriesDataRawReadEventArgs(this.topicConsumer, streamConsumer, timeseriesDataRaw));
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamConsumer.OnParameterDefinitionsChanged -= OnParameterDefinitionsChangedEventHandler;
            this.streamConsumer.OnTimeseriesData -= OnTimeseriesDataEventHandler;
            this.streamConsumer.OnTimeseriesData -= OnTimeseriesDataRawEventHandler;
        }

        /// <summary>
        /// Creates and returns an asynchronous enumerator for processing timeseries data.
        /// </summary>
        /// <param name="cancellationToken">An optional CancellationToken to cancel the operation.</param>
        /// <returns>An async iterator of TimeseriesDataTimestamp in the stream.</returns>
        public IAsyncEnumerator<TimeseriesDataTimestamp> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
        {
            var enumerator = new StreamTimeseriesConsumerAsyncDataEnumerator(this, this.streamConsumer, cancellationToken);
            return enumerator;
        }
    }

    /// <summary>
    /// StreamTimeseriesConsumerAsyncDataEnumerator is an internal class that implements
    /// an asynchronous enumerator for processing timeseries data from a streaming source.
    /// </summary>
    internal class StreamTimeseriesConsumerAsyncDataEnumerator : IAsyncEnumerator<TimeseriesDataTimestamp>
    {
        // Semaphores to synchronize the consumption of data and manage concurrency
        private readonly SemaphoreSlim dataAvailableSemaphore;
        private readonly SemaphoreSlim dataProcessedSemaphore;

        // CancellationTokenSource to handle cancellation of the enumerator
        private readonly CancellationTokenSource cts;

        // The current data iterator
        private IEnumerator<TimeseriesDataTimestamp> dataIterator = null;

        // Reference to the associated StreamTimeseriesConsumer
        private readonly StreamTimeseriesConsumer timeseriesConsumer;

        // Flag to track whether the enumerator has been disposed
        private bool disposed = false;

        // Reference to the associated IStreamConsumerInternal
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of the StreamTimeseriesConsumerAsyncDataEnumerator class.
        /// </summary>
        /// <param name="timeseriesConsumer">The associated StreamTimeseriesConsumer.</param>
        /// <param name="streamConsumer">The associated IStreamConsumerInternal.</param>
        /// <param name="cancellationToken">An optional CancellationToken to cancel the operation.</param>
        internal StreamTimeseriesConsumerAsyncDataEnumerator(StreamTimeseriesConsumer timeseriesConsumer,
            IStreamConsumerInternal streamConsumer, CancellationToken cancellationToken = default)
        {
            // Initialize the semaphore to signal when data is available for consumption.
            // Initial state is blocked (0), with a maximum count of 1.
            this.dataAvailableSemaphore = new SemaphoreSlim(0, 1);

            // Initialize the semaphore to signal when the data has been processed.
            // Initial state is blocked (0), with a maximum count of 1.
            this.dataProcessedSemaphore = new SemaphoreSlim(0, 1);
            
            this.cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            this.timeseriesConsumer = timeseriesConsumer;
            timeseriesConsumer.OnDataReceived += ConsumerOnOnDataReceived;
            this.streamConsumer = streamConsumer;
            streamConsumer.OnStreamClosed += StreamClosedHandler;
        }

        /// <summary>
        /// Handles the OnStreamClosed event.
        /// </summary>
        private void StreamClosedHandler(object sender, StreamClosedEventArgs e)
        {
            Dispose();
        }

        /// <summary>
        /// Handles the OnDataReceived event.
        /// </summary>
        private void ConsumerOnOnDataReceived(object sender, TimeseriesDataReadEventArgs args)
        {
            dataIterator = args.Data.Timestamps.GetEnumerator();
            dataAvailableSemaphore.Release();
            dataProcessedSemaphore.Wait(cts.Token); // Wait for it to be processed
        }

        /// <summary>
        /// Disposes the enumerator and releases associated resources.
        /// </summary>
        private void Dispose()
        {
            if (this.disposed) return;
            this.disposed = true;
            this.timeseriesConsumer.OnDataReceived -= ConsumerOnOnDataReceived;
            this.streamConsumer.OnStreamClosed -= StreamClosedHandler;
            cts.Cancel();
            this.dataAvailableSemaphore.Dispose();
            this.dataProcessedSemaphore.Dispose();
        }

        /// <summary>
        /// Asynchronously disposes the enumerator and releases associated resources.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            Dispose();

            return new ValueTask(Task.CompletedTask);
        }

        /// <summary>
        /// Asynchronously moves to the next element in the timeseries data iterator.
        /// This method will continue attempting to move to the next element until the stream is closed,
        /// the enumerator is disposed, or data is available.
        /// </summary>
        /// <returns>A ValueTask representing whether the enumerator successfully moved to the next element.</returns>
        public async ValueTask<bool> MoveNextAsync()
        {
            if (this.cts.IsCancellationRequested) return false;
            
            // Enter an infinite loop to keep trying to move to the next data element.
            while (true)
            {
                // If the dataIterator is not initialized, wait for data to become available.
                if (dataIterator == null)
                {
                    try
                    {
                        await dataAvailableSemaphore.WaitAsync(cts.Token);
                    }
                    catch (OperationCanceledException ex)
                    {
                        return false; // Throw exception?
                    }

                    if (this.cts.IsCancellationRequested) return false; // Could be disposed

                    Debug.Assert(dataIterator != null, "dataIterator is null");
                }
                
                if (dataIterator.MoveNext())
                {
                    this.Current = dataIterator.Current;
                    return true;
                }

                // If there are no more elements, release the dataProcessedSemaphore.
                dataIterator = null;
                dataProcessedSemaphore.Release();
            }
        }

        /// <summary>
        /// Gets the current TimeseriesDataTimestamp element in the enumerator.
        /// </summary>
        public TimeseriesDataTimestamp Current { get; private set; }
    }

    public class ParameterDefinitionsChangedEventArgs
    {
        public ParameterDefinitionsChangedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
        }

        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
    }

    public class TimeseriesDataReadEventArgs
    {
        public TimeseriesDataReadEventArgs(object topic, object stream, TimeseriesData data)
        {
            this.Topic = topic;
            this.Stream = stream;
            this.Data = data;
        }
        
        /// <summary>
        /// Topic of type <see cref="ITopicConsumer"/> or <see cref="ITopicProducer"/>
        /// </summary>
        public object Topic { get; }
        
        /// <summary>
        /// Stream of type <see cref="IStreamConsumer"/> or <see cref="IStreamProducer"/>
        /// </summary>
        public object Stream { get; }
        
        public TimeseriesData Data { get; }
    }
    
    public class TimeseriesDataRawReadEventArgs
    {
        public TimeseriesDataRawReadEventArgs(object topic, object stream, TimeseriesDataRaw data)
        {
            this.Topic = topic;
            this.Stream = stream;
            this.Data = data;
        }

        /// <summary>
        /// Topic of type <see cref="ITopicConsumer"/> or <see cref="ITopicProducer"/>
        /// </summary>
        public object Topic { get; }
        
        /// <summary>
        /// Stream of type <see cref="IStreamConsumer"/> or <see cref="IStreamProducer"/>
        /// </summary>
        public object Stream { get; }
        
        public TimeseriesDataRaw Data { get; }
    }
}
