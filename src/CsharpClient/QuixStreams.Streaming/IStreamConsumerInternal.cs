using System;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Stream reader interface. Stands for a new stream read from the platform.
    /// Allows to read the stream data received from a topic.
    /// </summary>
    internal interface IStreamConsumerInternal: IStreamConsumer
    {
        /// <summary>
        /// Event raised when the Stream Properties have changed.
        /// </summary>
        event Action<IStreamConsumer, QuixStreams.Telemetry.Models.StreamProperties> OnStreamPropertiesChanged;

        /// <summary>
        /// Event raised when the <see cref="Telemetry.Models.ParameterDefinitions"/> have been changed.
        /// </summary>
        event Action<IStreamConsumer, QuixStreams.Telemetry.Models.ParameterDefinitions> OnParameterDefinitionsChanged;

        /// <summary>
        /// Event raised when the <see cref="Telemetry.Models.EventDefinitions"/> have been changed.
        /// </summary>
        event Action<IStreamConsumer, QuixStreams.Telemetry.Models.EventDefinitions> OnEventDefinitionsChanged;

        /// <summary>
        /// Event raised when a new package of <see cref="TimeseriesDataRaw"/> values have been received.
        /// </summary>
        event Action<IStreamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw> OnTimeseriesData;

        /// <summary>
        /// Event raised when a new package of <see cref="EventDataRaw"/> values have been received.
        /// </summary>
        event Action<IStreamConsumer, QuixStreams.Telemetry.Models.EventDataRaw> OnEventData;
    }
}