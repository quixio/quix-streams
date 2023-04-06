using System;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// Builder for creating <see cref="EventDefinitions"/> within <see cref="StreamEventsProducer"/>
    /// </summary>
    public class EventDefinitionBuilder
    {
        private readonly StreamEventsProducer streamEventsProducer;
        private readonly string location;
        private QuixStreams.Telemetry.Models.EventDefinition properties;

        /// <summary>
        /// Initializes a new instance of <see cref="EventDefinitionBuilder"/>
        /// </summary>
        /// <param name="streamEventsProducer">Events producer owner</param>
        /// <param name="location">Location selected for the Event definition builder</param>
        /// <param name="properties">Events definition instance managed by the builder</param>
        public EventDefinitionBuilder(StreamEventsProducer streamEventsProducer, string location, QuixStreams.Telemetry.Models.EventDefinition properties = null)
        {
            this.streamEventsProducer = streamEventsProducer;
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
        /// <param name="eventId">Event id. This must match the event id you use to Event values</param>
        /// <param name="name">Human friendly display name of the event</param>
        /// <param name="description">Description of the event</param>
        /// <returns>Event definition builder to define the event properties</returns>
        public EventDefinitionBuilder AddDefinition(string eventId, string name = null, string description = null)
        {
            this.properties = this.streamEventsProducer.CreateDefinition(this.location, eventId, name, description);

            return this;
        }

    }

}
