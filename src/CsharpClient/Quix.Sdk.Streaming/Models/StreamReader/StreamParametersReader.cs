using System;
using System.Collections.Generic;
using System.Linq;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models.StreamReader
{
    /// <summary>
    /// Helper class for reader <see cref="ParameterDefinitions"/> and <see cref="TimeseriesData"/>
    /// </summary>
    public class StreamParametersReader : IDisposable
    {
        private readonly IInputTopic topic;
        private readonly IStreamReaderInternal streamReader;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamParametersReader"/>
        /// </summary>
        /// <param name="topic">The topic the stream to what this reader belongs to</param>
        /// <param name="streamReader">Stream reader owner</param>
        internal StreamParametersReader(IInputTopic topic, IStreamReaderInternal streamReader)
        {
            this.topic = topic;
            this.streamReader = streamReader;

            this.streamReader.OnParameterDefinitionsChanged += OnParameterDefinitionsChangedEventHandler;

            this.streamReader.OnTimeseriesData += OnTimeseriesDataEventHandler;
            this.streamReader.OnTimeseriesData += OnTimeseriesDataRawEventHandler;
        }

        private void OnParameterDefinitionsChangedEventHandler(IStreamReader sender, ParameterDefinitions parameterDefinitions)
        {
            this.LoadFromProcessDefinitions(parameterDefinitions);

            this.OnDefinitionsChanged?.Invoke(this.streamReader, new ParameterDefinitionsChangedEventArgs(this.topic, this.streamReader));
        }

        /// <summary>
        /// Create a new Parameters buffer for reading data
        /// </summary>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <returns>Parameters reading buffer</returns>
        public TimeseriesBufferReader CreateBuffer(TimeseriesBufferConfiguration bufferConfiguration = null, params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferReader(this.topic, this.streamReader, bufferConfiguration, parametersFilter);
            this.Buffers.Add(buffer);

            return buffer;
        }

        /// <summary>
        /// Create a new Parameters buffer for reading data
        /// </summary>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <returns>Parameters reading buffer</returns>
        public TimeseriesBufferReader CreateBuffer(params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferReader(this.topic, this.streamReader, null, parametersFilter);
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
        public event EventHandler<TimeseriesDataReadEventArgs> OnRead;

        /// <summary>
        /// Event raised when data is available to read (without buffering) in raw transport format
        /// This event does not use Buffers and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesDataRawReadEventArgs> OnReadRaw;

        /// <summary>
        /// Gets the latest set of event definitions
        /// </summary>
        public List<ParameterDefinition> Definitions { get; private set; } = new List<ParameterDefinition>();

        /// <summary>
        /// List of buffers created for this stream
        /// </summary>
        internal List<TimeseriesBufferReader> Buffers { get; private set; } = new List<TimeseriesBufferReader>();

        private void LoadFromProcessDefinitions(Process.Models.ParameterDefinitions definitions)
        {
            var defs = new List<ParameterDefinition>();
            
            if (definitions.Parameters != null) 
                this.ConvertParameterDefinitions(definitions.Parameters, "").ForEach(d => defs.Add(d));
            if (definitions.ParameterGroups != null)
                this.ConvertGroupParameterDefinitions(definitions.ParameterGroups, "").ForEach(d => defs.Add(d));

            this.Definitions = defs;
        }

        private List<ParameterDefinition> ConvertParameterDefinitions(List<Process.Models.ParameterDefinition> parameterDefinitions, string location)
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

        private List<ParameterDefinition> ConvertGroupParameterDefinitions(List<Process.Models.ParameterGroupDefinition> parameterGroupDefinitions, string location)
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

        private void OnTimeseriesDataEventHandler(IStreamReader streamReader, Process.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            var tsdata = new TimeseriesData(timeseriesDataRaw, null, false, false);
            this.OnRead?.Invoke(streamReader, new TimeseriesDataReadEventArgs(this.topic, streamReader, tsdata));
        }

        private void OnTimeseriesDataRawEventHandler(IStreamReader streamReader, Process.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            this.OnReadRaw?.Invoke(streamReader, new TimeseriesDataRawReadEventArgs(this.topic, streamReader, timeseriesDataRaw));
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamReader.OnParameterDefinitionsChanged -= OnParameterDefinitionsChangedEventHandler;
            this.streamReader.OnTimeseriesData -= OnTimeseriesDataEventHandler;
            this.streamReader.OnTimeseriesData -= OnTimeseriesDataRawEventHandler;
        }
    }

    public class ParameterDefinitionsChangedEventArgs
    {
        public ParameterDefinitionsChangedEventArgs(IInputTopic topic, IStreamReader reader)
        {
            this.Topic = topic;
            this.Stream = reader;
        }

        public IInputTopic Topic { get; }
        public IStreamReader Stream { get; }
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
        /// Topic of type <see cref="IInputTopic"/> or <see cref="IOutputTopic"/>
        /// </summary>
        public object Topic { get; }
        
        /// <summary>
        /// Stream of type <see cref="IStreamReader"/> or <see cref="IStreamWriter"/>
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
        /// Topic of type <see cref="IInputTopic"/> or <see cref="IOutputTopic"/>
        /// </summary>
        public object Topic { get; }
        
        /// <summary>
        /// Stream of type <see cref="IStreamReader"/> or <see cref="IStreamWriter"/>
        /// </summary>
        public object Stream { get; }
        
        public TimeseriesDataRaw Data { get; }
    }
}
