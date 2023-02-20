using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models.StreamReader
{
    /// <summary>
    /// Class used to read from the stream in a buffered manner
    /// </summary>
    public class TimeseriesBufferReader: TimeseriesBuffer, IDisposable
    {
        private readonly IStreamReaderInternal streamReader;
        private readonly string[] parametersFilter;

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesBufferReader"/>
        /// </summary>
        /// <param name="streamReader">Stream reader owner</param>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        internal TimeseriesBufferReader(IStreamReaderInternal streamReader, TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter)
            : base(bufferConfiguration, parametersFilter, true, false)
        {
            this.streamReader = streamReader;
            this.parametersFilter = parametersFilter;

            this.streamReader.OnTimeseriesData += OnTimeseriesData;
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            this.streamReader.OnTimeseriesData -= OnTimeseriesData;
            base.Dispose();
        }

        private void OnTimeseriesData(IStreamReaderInternal streamReader, Process.Models.TimeseriesDataRaw TimeseriesDataRaw)
        {
            this.WriteChunk(TimeseriesDataRaw);
        }

        protected override void InvokeOnRead(object sender, TimeseriesData TimeseriesData)
        {
            base.InvokeOnRead(this.streamReader, TimeseriesData);
        }

        protected override void InvokeOnReadRaw(object sender, TimeseriesDataRaw TimeseriesDataRaw)
        {
            base.InvokeOnReadRaw(this.streamReader, TimeseriesDataRaw);
        }
    }
}
