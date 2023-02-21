using System;
using System.Collections.Generic;
using System.Threading;
using Quix.Sdk.Process.Managers;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process.Models.Utility;
using Quix.Sdk.Streaming.Exceptions;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Helper class for writing <see cref="ParameterDefinition"/> and <see cref="TimeseriesData"/>
    /// </summary>
    public class StreamParametersWriter : IDisposable
    {
        private readonly IStreamWriterInternal streamWriter;

        private readonly ILogger logger = Logging.CreateLogger<StreamParametersWriter>();

        private string location;
        private readonly ParameterDefinitionsManager parameterDefinitionsManager = new ParameterDefinitionsManager();
        private readonly Timer flushDefinitionsTimer;
        private bool timerEnabled = false; // Here because every now and then resetting its due time to never doesn't work
        private bool isDisposed;
        private const int TimerInterval = 20;
        private readonly object flushLock = new object();

        /// <summary>
        /// Initializes a new instance of <see cref="StreamParametersWriter"/>
        /// </summary>
        /// <param name="outputTopic">The output topic the writer will write to</param>
        /// <param name="streamWriter">Stream writer owner</param>
        internal StreamParametersWriter(IOutputTopic outputTopic, IStreamWriterInternal streamWriter)
        {
            this.streamWriter = streamWriter;

            // Parameters Buffer 
            this.Buffer = new TimeseriesBufferWriter(outputTopic, this.streamWriter, new TimeseriesBufferConfiguration());

            // Timer for Flush Parameter definitions
            flushDefinitionsTimer = new Timer(OnFlushDefinitionsTimerEvent, null, Timeout.Infinite, Timeout.Infinite); // Create disabled flush timer

            // Initialize root location
            this.DefaultLocation = "/";
        }

        /// <summary>
        /// Gets the buffer for writing timeseries data
        /// </summary>
        public TimeseriesBufferWriter Buffer { get;  }

        /// <summary>
        /// Write data to stream without using Buffer
        /// </summary>
        /// <param name="data">Timeseries data to write</param>
        public void Write(TimeseriesData data)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }

            for (var index = 0; index < data.Timestamps.Count; index++)
            {
                var timestamp = data.Timestamps[index];

                if (!timestamp.EpochIncluded)
                {
                    timestamp.TimestampNanoseconds += this.streamWriter.Epoch.ToUnixNanoseconds();
                    timestamp.EpochIncluded = true;
                }
            }

            this.streamWriter.Write(data.ConvertToProcessData());
        }

        /// <summary>
        /// Write data timeseries data raw directly to stream
        /// </summary>
        /// <param name="data">Timeseries data to write</param>
        public void Write(Process.Models.TimeseriesDataRaw data)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }

            long epochDiff = this.streamWriter.Epoch.ToUnixNanoseconds();

            if (epochDiff == 0)
            {
                // No epoch modification needed >> directly write to the stream
                this.streamWriter.Write(data);
                return;
            }

            long[] updatedTimestamps = new long[data.Timestamps.Length];
            for (int i = 0; i < updatedTimestamps.Length; i++) 
            { 
                updatedTimestamps[i] = data.Timestamps[i] + epochDiff;
            }

            Process.Models.TimeseriesDataRaw new_data = new Process.Models.TimeseriesDataRaw(
                data.Epoch, 
                updatedTimestamps, 
                data.NumericValues, 
                data.StringValues, 
                data.BinaryValues, 
                data.TagValues
            );

            this.streamWriter.Write(new_data);
        }


        /// <summary>
        /// Default Location of the parameters. Parameter definitions added with <see cref="AddDefinition"/> will be inserted at this location.
        /// See <see cref="AddLocation"/> for adding definitions at a different location without changing default.
        /// Example: "/Group1/SubGroup2"
        /// </summary>
        public string DefaultLocation
        {
            get
            {
                return this.location;
            }
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamParametersWriter));
                }
                this.location = this.parameterDefinitionsManager.ReformatLocation(value);
            }
        }

        /// <summary>
        /// Adds a list of definitions to the <see cref="StreamParametersWriter"/>. Configure it with the builder methods.
        /// </summary>
        /// <param name="definitions">List of definitions</param>
        public void AddDefinitions(List<ParameterDefinition> definitions)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }
            definitions.ForEach(d => this.parameterDefinitionsManager.AddDefinition(d.ConvertToProcessDefinition(), d.Location));

            this.ResetFlushDefinitionsTimer();
        }

        /// <summary>
        /// Adds a new parameter definition to the <see cref="StreamParametersWriter"/>. Configure it with the builder methods.
        /// </summary>
        /// <param name="parameterId">The id of the parameter. Must match the parameter id used to send data.</param>
        /// <param name="name">The human friendly display name of the parameter</param>
        /// <param name="description">The description of the parameter</param>
        /// <returns>Parameter definition builder to define the parameter properties</returns>
        public ParameterDefinitionBuilder AddDefinition(string parameterId, string name = null, string description = null)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }
            var parameterDefinition = this.CreateDefinition(this.location, parameterId, name, description);

            var builder = new ParameterDefinitionBuilder(this, this.location, parameterDefinition);

            return builder;
        }

        /// <summary>
        /// Adds a new location in the parameters groups hierarchy
        /// </summary>
        /// <param name="location">The group location</param>
        /// <returns>Parameter definition builder to define the parameters under the specified location</returns>
        public ParameterDefinitionBuilder AddLocation(string location)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }
            this.parameterDefinitionsManager.GenerateLocations(location);

            var builder = new ParameterDefinitionBuilder(this, location);

            return builder;
        }

        internal Process.Models.ParameterDefinition CreateDefinition(string location, string parameterId, string name, string description)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }
            var parameterDefinition = new Process.Models.ParameterDefinition
            {
                Id = parameterId,
                Name = name,
                Description = description
            };

            this.parameterDefinitionsManager.AddDefinition(parameterDefinition, location);

            this.ResetFlushDefinitionsTimer();

            return parameterDefinition;
        }

        /// <summary>
        /// Immediately writes the timeseries data and definitions from the buffer without waiting for buffer condition to fulfill for either
        /// </summary>
        public void Flush()
        {
            this.Flush(false);
        }

        private void Flush(bool force)
        {
            if (!force && isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamParametersWriter));
            }

            try
            {
                lock (flushLock)
                {
                    this.FlushDefinitions();
                    this.Buffer.Flush();
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Exception occurred while trying to flush timeseries data buffer.");
            }
        }

        private void ResetFlushDefinitionsTimer()
        {
            if (isDisposed) return;
            timerEnabled = true;
            flushDefinitionsTimer.Change(TimerInterval, Timeout.Infinite); // Reset / Enable timer
        }

        private void OnFlushDefinitionsTimerEvent(object state)
        {
            if (!this.timerEnabled) return;
            try
            {
                this.FlushDefinitions();
            }
            catch (StreamClosedException exception) when (this.isDisposed)
            {
                // Ignore exception because the timer flush definition may finish executing only after closure due to how close lock works in streamWriter
            }
            catch (Exception ex)
            {
                this.logger.Log(LogLevel.Error, ex, "Exception occurred while trying to flush parameter definition buffer.");
            }
        }

        private void FlushDefinitions()
        {
            timerEnabled = false;
            flushDefinitionsTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer

            var definitions = parameterDefinitionsManager.GenerateParameterDefinitions();

            if (definitions.Parameters?.Count == 0 && definitions.ParameterGroups?.Count == 0) return; // there is nothing to flush

            this.streamWriter.Write(definitions);
        }

        /// <summary>
        /// Flushes internal buffers and disposes
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed) return;
            this.isDisposed = true;
            this.Flush(true);
            flushDefinitionsTimer?.Dispose();
            Buffer?.Dispose();
        }
    }
}
