using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Helper class for reader <see cref="ParameterDefinitions"/> and <see cref="TimeseriesData"/>
    /// </summary>
    public class StreamTimeseriesConsumer : IDisposable, IEnumerable<TimeseriesDataTimestamp>, IAsyncEnumerable<TimeseriesDataTimestamp>
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
        /// Creates and returns a synchronous enumerator for processing timeseries data.
        /// </summary>
        /// <returns>An iterator of TimeseriesDataTimestamp in the stream.</returns>
        public IEnumerator<TimeseriesDataTimestamp> GetEnumerator()
        {
            var enumerator = new StreamTimeseriesConsumerDataEnumerator(this, this.streamConsumer);
            return enumerator;
        }

        /// <summary>
        /// Creates and returns a synchronous enumerator for processing timeseries data.
        /// </summary>
        /// <returns>An iterator of TimeseriesDataTimestamp in the stream.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
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

    internal class StreamTimeseriesConsumerDataEnumerator : IEnumerator<TimeseriesDataTimestamp>
    {
        private readonly ManualResetEventSlim dataProcessedMre;
        // Semaphores to synchronize the consumption of data and manage concurrency
        private readonly ManualResetEventSlim dataArrivedMre;

        // CancellationTokenSource to handle cancellation of the enumerator
        private readonly CancellationTokenSource cts;

        // The current data
        private TimeseriesData data;
        // The current data iterator
        private IEnumerator<TimeseriesDataTimestamp> dataIterator = null;

        // Reference to the associated StreamTimeseriesConsumer
        private readonly StreamTimeseriesConsumer timeseriesConsumer;

        // Flag to track whether the enumerator has been disposed
        private bool disposed = false;

        // Reference to the associated IStreamConsumerInternal
        private readonly IStreamConsumerInternal streamConsumer;

        private static long instanceCounter = 0;
        
        internal StreamTimeseriesConsumerDataEnumerator(StreamTimeseriesConsumer timeseriesConsumer,
            IStreamConsumerInternal streamConsumer, CancellationToken cancellationToken = default)
        {
            // Initialize the semaphore to signal when the data has been processed.
            // Initial state is blocked (0), with a maximum count of 1.
            this.dataArrivedMre = new ManualResetEventSlim(false);
            this.dataProcessedMre = new ManualResetEventSlim(false);
            
            this.cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            this.timeseriesConsumer = timeseriesConsumer;
            timeseriesConsumer.OnDataReceived += ConsumerOnOnDataReceived;
            this.streamConsumer = streamConsumer;
            streamConsumer.OnStreamClosed += StreamClosedHandler;
            Interlocked.Increment(ref instanceCounter);
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
            if (args.Data == null) return;
            if (args.Data.Timestamps.Count == 0) return;


            this.data = args.Data;

            this.dataProcessedMre.Reset(); // so we can wait on it
            this.dataArrivedMre.Set();


            this.dataProcessedMre.Wait(cts.Token); // Wait for it to be processed

        }
        
        public bool MoveNext()
        {
            if (this.cts.IsCancellationRequested) return false;

            var iteration = 0;
            
            // Enter an infinite loop to keep trying to move to the next data element.
            while (true)
            {
                iteration++;
                // If the dataIterator is not initialized, wait for data to become available.
                if (this.dataIterator == null)
                {
                    try
                    {
                        this.dataArrivedMre.Wait(cts.Token);
                        this.dataArrivedMre.Reset(); // so we can wait on it again
                    }
                    catch (OperationCanceledException ex)
                    {
                        return false; // Throw exception?
                    }

                    if (this.cts.IsCancellationRequested) return false; // Could be disposed


                    Debug.Assert(data != null, "dataIterator is null");
                    this.dataIterator = this.data.Timestamps.GetEnumerator();


                }
                
                if (dataIterator.MoveNext())
                {
                    this.Current = dataIterator.Current;
                    return true;
                }

                // If there are no more elements, release the dataProcessedSemaphore.
                this.dataIterator = null;
                this.data = null;
                this.dataProcessedMre.Set();
            }
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public TimeseriesDataTimestamp Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (this.disposed) return;
            this.disposed = true;
            this.timeseriesConsumer.OnDataReceived -= ConsumerOnOnDataReceived;
            this.streamConsumer.OnStreamClosed -= StreamClosedHandler;
            cts.Cancel();
            Interlocked.Decrement(ref instanceCounter);
        }
    }

    /// <summary>
    /// StreamTimeseriesConsumerAsyncDataEnumerator is an internal class that implements
    /// an asynchronous enumerator for processing timeseries data from a streaming source.
    /// </summary>
    internal class StreamTimeseriesConsumerAsyncDataEnumerator : IAsyncEnumerator<TimeseriesDataTimestamp>
    {
        // Semaphores to synchronize the consumption of data and manage concurrency
        private readonly SemaphoreSlim dataProcessedSemaphore;

        // CancellationTokenSource to handle cancellation of the enumerator
        private readonly CancellationTokenSource cts;

        // The current data
        private TimeseriesData data;
        // The current data iterator
        private IEnumerator<TimeseriesDataTimestamp> dataIterator = null;
        // The data channel used to communicate between threads
        private readonly Channel<TimeseriesData> dataChannel = Channel.CreateBounded<TimeseriesData>(1);

        // Reference to the associated StreamTimeseriesConsumer
        private readonly StreamTimeseriesConsumer timeseriesConsumer;

        // Flag to track whether the enumerator has been disposed
        private bool disposed = false;

        // Reference to the associated IStreamConsumerInternal
        private readonly IStreamConsumerInternal streamConsumer;

        private static long instanceCounter = 0;

        /// <summary>
        /// Initializes a new instance of the StreamTimeseriesConsumerAsyncDataEnumerator class.
        /// </summary>
        /// <param name="timeseriesConsumer">The associated StreamTimeseriesConsumer.</param>
        /// <param name="streamConsumer">The associated IStreamConsumerInternal.</param>
        /// <param name="cancellationToken">An optional CancellationToken to cancel the operation.</param>
        internal StreamTimeseriesConsumerAsyncDataEnumerator(StreamTimeseriesConsumer timeseriesConsumer,
            IStreamConsumerInternal streamConsumer, CancellationToken cancellationToken = default)
        {
            // Initialize the semaphore to signal when the data has been processed.
            // Initial state is blocked (0), with a maximum count of 1.
            this.dataProcessedSemaphore = new SemaphoreSlim(0, 1);
            
            this.cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            this.timeseriesConsumer = timeseriesConsumer;
            timeseriesConsumer.OnDataReceived += ConsumerOnOnDataReceived;
            this.streamConsumer = streamConsumer;
            streamConsumer.OnStreamClosed += StreamClosedHandler;
            Interlocked.Increment(ref instanceCounter);
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
            if (args.Data == null) return;
            if (args.Data.Timestamps.Count == 0) return;

            try
            {
                dataChannel.Writer.TryWrite(args.Data);
            }
            catch (ChannelClosedException)
            {
                // Handle the channel being closed
                return;
            }

            this.dataProcessedSemaphore.Wait(cts.Token); // Wait for it to be processed

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
            this.dataChannel.Writer.Complete();
            this.dataProcessedSemaphore.Dispose();
            Interlocked.Decrement(ref instanceCounter);
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

            var iteration = 0;
            
            // Enter an infinite loop to keep trying to move to the next data element.
            while (true)
            {
                iteration++;
                // If the dataIterator is not initialized, wait for data to become available.
                if (this.dataIterator == null)
                {
                    try
                    {
                        this.data = await this.dataChannel.Reader.ReadAsync(cts.Token);
                        this.dataIterator = this.data.Timestamps.GetEnumerator();
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
                this.dataIterator = null;
                this.data = null;
                this.dataProcessedSemaphore.Release();
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
