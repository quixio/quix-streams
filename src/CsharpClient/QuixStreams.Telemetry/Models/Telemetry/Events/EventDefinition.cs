using System;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Provides additional data for events
    /// </summary>
    public class EventDefinition
    {
        private string id;

        /// <summary>
        /// The globally unique identifier of the event.
        /// Must be same as <see cref="EventDataRaw.Id"/>
        /// </summary>
        public string Id
        {
            get => id;
            set
            {
                if (string.IsNullOrWhiteSpace(value)) throw new ArgumentOutOfRangeException(nameof(Id), "Event must have a unique Id");
                if (value.IndexOfAny(new char[] {'/', '\\'}) > -1)
                {
                    throw new ArgumentOutOfRangeException(nameof(Id), "Event Id must not contain the following characters: /\\");
                }
                id = value;
            }
        }

        /// <summary>
        /// The display name of the event
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// Description of the event
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// Optional field for any custom properties that do not exist on the event.
        /// For example this could be a json string, describing all possible event values
        /// </summary>
        public string CustomProperties { get; set; }
        
        /// <summary>
        /// The level of the event. Defaults to <see cref="EventLevel.Information"/>
        /// </summary>
        public EventLevel Level { get; set; } = EventLevel.Information;
    }
}