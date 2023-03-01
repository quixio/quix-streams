namespace Quix.Streams.Streaming.Models
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
        public Telemetry.Models.EventLevel Level { get; internal set; } = Telemetry.Models.EventLevel.Information;

        /// <summary>
        /// Converts the Event definition to Process layer structure
        /// </summary>
        /// <returns>Process layer Event definition</returns>
        internal Telemetry.Models.EventDefinition ConvertToProcessDefinition()
        {
            return new Telemetry.Models.EventDefinition 
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