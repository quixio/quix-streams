using Quix.Sdk.Process;
using System;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Stream reader interface. Stands for a new stream read from the platform.
    /// Allows to read the stream data received from a topic.
    /// </summary>
    internal interface IStreamReaderInternal
    {
        /// <summary>
        /// Event raised when the Stream Properties have changed.
        /// </summary>
        event Action<IStreamReaderInternal, Process.Models.StreamProperties> OnStreamPropertiesChanged;

        /// <summary>
        /// Event raised when the <see cref="Process.Models.ParameterDefinitions"/> have been changed.
        /// </summary>
        event Action<IStreamReaderInternal, Process.Models.ParameterDefinitions> OnParameterDefinitionsChanged;

        /// <summary>
        /// Event raised when the <see cref="Process.Models.EventDefinitions"/> have been changed.
        /// </summary>
        event Action<IStreamReaderInternal, Process.Models.EventDefinitions> OnEventDefinitionsChanged;

        /// <summary>
        /// Event raised when a new package of <see cref="TimeseriesDataRaw"/> values have been received.
        /// </summary>
        event Action<IStreamReaderInternal, Process.Models.TimeseriesDataRaw> OnTimeseriesData;

        /// <summary>
        /// Event raised when a new package of <see cref="EventDataRaw"/> values have been received.
        /// </summary>
        event Action<IStreamReaderInternal, Process.Models.EventDataRaw> OnEventData;

    }
}