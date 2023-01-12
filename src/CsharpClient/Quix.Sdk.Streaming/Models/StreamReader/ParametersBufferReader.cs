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
    public class ParametersBufferReader: ParametersBuffer, IDisposable
    {
        private readonly IStreamReaderInternal streamReader;
        private readonly string[] parametersFilter;

        /// <summary>
        /// Initializes a new instance of <see cref="ParametersBufferReader"/>
        /// </summary>
        /// <param name="streamReader">Stream reader owner</param>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        internal ParametersBufferReader(IStreamReaderInternal streamReader, ParametersBufferConfiguration bufferConfiguration, string[] parametersFilter)
            : base(bufferConfiguration, parametersFilter, true, false)
        {
            this.streamReader = streamReader;
            this.parametersFilter = parametersFilter;

            this.streamReader.OnParameterData += OnParameterData;
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            this.streamReader.OnParameterData -= OnParameterData;
            base.Dispose();
        }

        private void OnParameterData(IStreamReaderInternal streamReader, Process.Models.ParameterDataRaw parameterDataRaw)
        {
            this.WriteChunk(parameterDataRaw);
        }

    }
}
