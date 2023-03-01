using System;
using System.Collections.Generic;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Describes the location of events with extra optional context
    /// </summary>
    public class EventGroupDefinition
    {
        private string name;

        /// <summary>
        /// Human friendly display name of the group
        /// </summary>
        public string Name
        {
            get => name;
            set
            {
                if (string.IsNullOrWhiteSpace(value)) throw new ArgumentOutOfRangeException(nameof(Name), "Group must have a name");
                if (value.IndexOfAny(new char[] {'/', '\\'}) > -1) throw new ArgumentOutOfRangeException(nameof(Name), "Group name must not contain the following characters: /\\");
                name = value;
            }
        }

        /// <summary>
        /// Description of the group
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Optional field for any custom properties that do not exist on the group.
        /// For example this could be a json string, describing ordering information of the group.
        /// </summary>
        public string CustomProperties { get; set; }

        /// <summary>
        /// Groups below this group in the hierarchy
        /// </summary>
        public List<EventGroupDefinition> ChildGroups { get; set; }

        /// <summary>
        /// Events that belong to this group
        /// </summary>
        public List<EventDefinition> Events { get; set; }
    }
}