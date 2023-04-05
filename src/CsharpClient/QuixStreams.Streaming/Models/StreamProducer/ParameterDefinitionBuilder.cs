using System;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// Builder for creating <see cref="ParameterDefinition"/> for <see cref="StreamTimeseriesProducer"/>
    /// </summary>
    public class ParameterDefinitionBuilder
    {
        private readonly StreamTimeseriesProducer streamTimeseriesProducer;
        private readonly string location;
        private QuixStreams.Telemetry.Models.ParameterDefinition definition;

        /// <summary>
        /// Initializes a new instance of <see cref="ParameterDefinitionBuilder"/>
        /// </summary>
        /// <param name="streamTimeseriesProducer">Parameters producer owner</param>
        /// <param name="location">Location selected for the Parameter definition builder</param>
        /// <param name="definition">Parameter definition instance managed by the builder</param>
        public ParameterDefinitionBuilder(StreamTimeseriesProducer streamTimeseriesProducer, string location, QuixStreams.Telemetry.Models.ParameterDefinition definition = null)
        {
            this.streamTimeseriesProducer = streamTimeseriesProducer;
            this.location = location;
            this.definition = definition;
        }

        /// <summary>
        /// Set the minimum and maximum range of the parameter
        /// </summary>
        /// <param name="minimumValue">Minimum value</param>
        /// <param name="maximumValue">Maximum value</param>
        public ParameterDefinitionBuilder SetRange(double minimumValue, double maximumValue)
        {
            if (this.definition == null)
            {
                throw new Exception($"You must add a definition before calling this method.");
            }

            if (minimumValue > maximumValue)
            {
                throw new ArgumentOutOfRangeException($"Maximum value ({maximumValue}) definition must be greater than minimum value ({minimumValue}) definition. Parameter = {this.definition.Id}");
            }

            if (double.IsInfinity(minimumValue) || double.IsInfinity(maximumValue) || double.IsNaN(minimumValue) || double.IsNaN(maximumValue))
            {
                throw new ArgumentOutOfRangeException($"Minimum value ({minimumValue}) or maximum value ({maximumValue}) is out of range. Parameter = {this.definition.Id}");
            }

            this.definition.MinimumValue = minimumValue;
            this.definition.MaximumValue = maximumValue;

            return this;
        }

        /// <summary>
        /// Set the unit of the parameter
        /// </summary>
        /// <param name="unit">Unit of the parameter</param>
        public ParameterDefinitionBuilder SetUnit(string unit)
        {
            if (this.definition == null)
            {
                throw new Exception($"You must add a definition before calling this method.");
            }

            this.definition.Unit = unit;

            return this;
        }

        /// <summary>
        /// Set the format of the parameter
        /// </summary>
        /// <param name="format">The formatting to apply on the value for display purposes</param>
        public ParameterDefinitionBuilder SetFormat(string format)
        {
            if (this.definition == null)
            {
                throw new Exception($"You must add a definition before calling this method.");
            }

            this.definition.Format = format;

            return this;
        }

        /// <summary>
        /// Set the custom properties of the parameter
        /// </summary>
        /// <param name="customProperties">The custom properties of the parameter</param>
        public ParameterDefinitionBuilder SetCustomProperties(string customProperties)
        {
            if (this.definition == null)
            {
                throw new Exception($"You must add a definition before calling this method.");
            }

            this.definition.CustomProperties = customProperties;

            return this;
        }

        /// <summary>
        /// Add new parameter definition to the <see cref="StreamTimeseriesProducer"/>. Configure it with the builder methods.
        /// </summary>
        /// <param name="parameterId">The id of the parameter. Must match the parameter id used to send data.</param>
        /// <param name="name">The human friendly display name of the parameter</param>
        /// <param name="description">The description of the parameter</param>
        /// <returns>Parameter definition builder to define the parameter properties</returns>
        public ParameterDefinitionBuilder AddDefinition(string parameterId, string name = null, string description = null)
        {
            this.definition = this.streamTimeseriesProducer.CreateDefinition(this.location, parameterId, name, description);

            return this;
        }

    }

}
