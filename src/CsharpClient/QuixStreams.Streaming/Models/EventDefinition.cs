namespace QuixStreams.Streaming.Models
{

    /// <summary>
    /// Describes additional context for the event
    /// </summary>
    public class EventDefinition
    {
        /// <summary>
        /// Gets the globally unique identifier of the event.
        /// </summary>
        public string Id { get; internal set;  }

        /// <summary>
        /// Gets the display name of the event
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Gets the description of the event
        /// </summary>
        public string Description { get; internal set; }

        /// <summary>
        /// Gets the location of the event within the Event hierarchy. Example: "/", "car/chassis/suspension".
        /// </summary>
        public string Location { get; internal set; }

        /// <summary>
        /// Gets the optional field for any custom properties that do not exist on the event.
        /// For example this could be a json string, describing all possible event values
        /// </summary>
        public string CustomProperties { get; internal set; }

        /// <summary>
        /// Gets the level of the event. Defaults to <see cref="Telemetry.Models.EventLevel.Information"/>
        /// </summary>
        public QuixStreams.Telemetry.Models.EventLevel Level { get; internal set; } = QuixStreams.Telemetry.Models.EventLevel.Information;

        /// <summary>
        /// Converts the Event definition to Telemetry layer structure
        /// </summary>
        /// <returns>Telemetry layer Event definition</returns>
        internal QuixStreams.Telemetry.Models.EventDefinition ConvertToTelemetryDefinition()
        {
            return new QuixStreams.Telemetry.Models.EventDefinition 
            {
                Id = this.Id,
                Name = this.Name,
                Description = this.Description,
                CustomProperties = this.CustomProperties,
                Level = this.Level
            };
        }
    }
}