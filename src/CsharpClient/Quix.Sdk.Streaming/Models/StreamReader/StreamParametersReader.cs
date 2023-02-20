using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models.StreamReader
{
    /// <summary>
    /// Helper class for reader <see cref="ParameterDefinitions"/> and <see cref="TimeseriesData"/>
    /// </summary>
    public class StreamParametersReader : IDisposable
    {
        private readonly IStreamReaderInternal streamReader;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamParametersReader"/>
        /// </summary>
        /// <param name="streamReader">Stream reader owner</param>
        internal StreamParametersReader(IStreamReaderInternal streamReader)
        {
            this.streamReader = streamReader;

            this.streamReader.OnParameterDefinitionsChanged += OnStreamReaderOnOnParameterDefinitionsChanged;

            this.streamReader.OnTimeseriesData += OnTimeseriesData;
            this.streamReader.OnTimeseriesData += OnTimeseriesDataRaw;
        }

        private void OnStreamReaderOnOnParameterDefinitionsChanged(IStreamReaderInternal sender, ParameterDefinitions parameterDefinitions)
        {
            this.LoadFromProcessDefinitions(parameterDefinitions);

            this.OnDefinitionsChanged?.Invoke(this.streamReader, EventArgs.Empty);
        }

        /// <summary>
        /// Create a new Parameters buffer for reading data
        /// </summary>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <returns>Parameters reading buffer</returns>
        public TimeseriesBufferReader CreateBuffer(TimeseriesBufferConfiguration bufferConfiguration = null, params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferReader(this.streamReader, bufferConfiguration, parametersFilter);
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
            var buffer = new TimeseriesBufferReader(this.streamReader, null, parametersFilter);
            this.Buffers.Add(buffer);

            return buffer;
        }

        /// <summary>
        /// Raised when the parameter definitions have changed for the stream.
        /// See <see cref="Definitions"/> for the latest set of parameter definitions
        /// </summary>
        public event EventHandler OnDefinitionsChanged;

        /// <summary>
        /// Event raised when data is available to read (without buffering)
        /// This event does not use Buffers and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesData> OnRead;

        /// <summary>
        /// Event raised when data is available to read (without buffering) in raw transport format
        /// This event does not use Buffers and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesDataRaw> OnReadRaw;

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

        private void OnTimeseriesData(IStreamReaderInternal streamReader, Process.Models.TimeseriesDataRaw TimeseriesDataRaw)
        {
            var tsdata = new TimeseriesData(TimeseriesDataRaw, null, false, false);
            this.OnRead?.Invoke(streamReader, tsdata);
        }

        private void OnTimeseriesDataRaw(IStreamReaderInternal streamReader, Process.Models.TimeseriesDataRaw TimeseriesDataRaw)
        {
            this.OnReadRaw?.Invoke(streamReader, TimeseriesDataRaw);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamReader.OnParameterDefinitionsChanged -= OnStreamReaderOnOnParameterDefinitionsChanged;
            this.streamReader.OnTimeseriesData -= OnTimeseriesData;
            this.streamReader.OnTimeseriesData -= OnTimeseriesDataRaw;
        }
    }
}
