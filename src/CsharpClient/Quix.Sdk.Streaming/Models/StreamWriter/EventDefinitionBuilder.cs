using Quix.Sdk.Process.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Builder for creating <see cref="EventDefinitions"/> within <see cref="StreamEventsWriter"/>
    /// </summary>
    public class EventDefinitionBuilder
    {
        private readonly StreamEventsWriter streamEventsWriter;
        private readonly string location;
        private Process.Models.EventDefinition properties;

        /// <summary>
        /// Initializes a new instance of <see cref="EventDefinitionBuilder"/>
        /// </summary>
        /// <param name="streamEventsWriter">Events writer owner</param>
        /// <param name="location">Location selected for the Event definition builder</param>
        /// <param name="properties">Events definition instance managed by the builder</param>
        public EventDefinitionBuilder(StreamEventsWriter streamEventsWriter, string location, Process.Models.EventDefinition properties = null)
        {
            this.streamEventsWriter = streamEventsWriter;
            this.location = location;
            this.properties = properties;
        }

        /// <summary>
        /// Set severity level of the Event
        /// </summary>
        /// <param name="level">The severity level of the event</param>
        public EventDefinitionBuilder SetLevel(EventLevel level)
        {
            if (this.properties == null)
            {
                throw new Exception($"You must add a definition before calling this method.");
            }

            this.properties.Level = level;

            return this;
        }

        /// <summary>
        /// Set custom properties of the Event
        /// </summary>
        /// <param name="customProperties"></param>
        public EventDefinitionBuilder SetCustomProperties(string customProperties)
        {
            if (this.properties == null)
            {
                throw new Exception($"You must add a definition before calling this method.");
            }

            this.properties.CustomProperties = customProperties;

            return this;
        }

        /// <summary>
        /// Add new Event definition, to define properties like Name or Level, among others.
        /// </summary>
        /// <param name="eventId">Event Id. This must match the event id you use to Event values</param>
        /// <param name="name">Human friendly display name of the event</param>
        /// <param name="description">Description of the event</param>
        /// <returns>Event definition builder to define the event properties</returns>
        public EventDefinitionBuilder AddDefinition(string eventId, string name = null, string description = null)
        {
            this.properties = this.streamEventsWriter.CreateDefinition(this.location, eventId, name, description);

            return this;
        }

    }

}
