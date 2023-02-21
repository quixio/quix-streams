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
        private readonly IInputTopic inputTopic;
        private readonly IStreamReaderInternal streamReader;
        private readonly string[] parametersFilter;

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesBufferReader"/>
        /// </summary>
        /// <param name="inputTopic">The topic it belongs to</param>
        /// <param name="streamReader">Stream reader owner</param>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        internal TimeseriesBufferReader(IInputTopic inputTopic, IStreamReaderInternal streamReader, TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter)
            : base(bufferConfiguration, parametersFilter, true, false)
        {
            this.inputTopic = inputTopic;
            this.streamReader = streamReader;
            this.parametersFilter = parametersFilter;

            this.streamReader.OnTimeseriesData += OnTimeseriesDataEventHandler;
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            this.streamReader.OnTimeseriesData -= OnTimeseriesDataEventHandler;
            base.Dispose();
        }

        private void OnTimeseriesDataEventHandler(IStreamReader streamReader, Process.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            this.WriteChunk(timeseriesDataRaw);
        }

        protected override void InvokeOnRead(object sender, TimeseriesDataReadEventArgs args)
        {
            base.InvokeOnRead(this, new TimeseriesDataReadEventArgs(this.inputTopic, this.streamReader, args.Data));
        }

        protected override void InvokeOnRawRead(object sender, TimeseriesDataRawReadEventArgs args)
        {
            base.InvokeOnRawRead(this, new TimeseriesDataRawReadEventArgs(this.inputTopic, this.streamReader, args.Data));
        }
    }
}
