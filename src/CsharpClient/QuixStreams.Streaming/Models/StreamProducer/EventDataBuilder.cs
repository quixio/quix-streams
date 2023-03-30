using System.Collections.Generic;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// Builder for creating <see cref="EventData"/> packages within the <see cref="StreamEventsProducer"/> 
    /// </summary>
    public class EventDataBuilder
    {
        private readonly StreamEventsProducer streamEventsProducer;
        private readonly long timestampNanoseconds;

        private readonly List<QuixStreams.Telemetry.Models.EventDataRaw> events = new List<QuixStreams.Telemetry.Models.EventDataRaw>();
        private readonly Dictionary<string, string> tags;

        /// <summary>
        /// Initializes a new instance of <see cref="EventDataBuilder"/>
        /// </summary>
        /// <param name="streamEventsProducer">Events producer owner</param>
        /// <param name="timestampNanoseconds">Timestamp assigned to the Events created by the builder</param>
        public EventDataBuilder(StreamEventsProducer streamEventsProducer, long timestampNanoseconds)
        {
            this.streamEventsProducer = streamEventsProducer;
            this.timestampNanoseconds = timestampNanoseconds;
            this.tags = new Dictionary<string, string>(this.streamEventsProducer.DefaultTags);
        }

        /// <summary>
        /// Adds new event value at the time the builder is created for
        /// </summary>
        /// <param name="eventId">Event Id</param>
        /// <param name="value">Value of the event</param>
        /// <returns></returns>
        public EventDataBuilder AddValue(string eventId, string value)
        {
            var @event = new QuixStreams.Telemetry.Models.EventDataRaw()
            {
                Id = eventId,
                Timestamp = timestampNanoseconds,
                Value = value
            };

            this.events.Add(@event);

            return this;
        }

        /// <summary>
        /// Adds a tag to the event values
        /// </summary>
        /// <param name="tagId">Tag Id</param>
        /// <param name="value">Tag value</param>
        public EventDataBuilder AddTag(string tagId, string value)
        {
            this.tags[tagId] = value;

            return this;
        }
        
        /// <summary>
        /// Adds tags to the event values.
        /// </summary>
        /// <param name="tagsValues">Tags values.</param>
        public EventDataBuilder AddTags(IEnumerable<KeyValuePair<string, string>> tagsValues)
        {
            if (tagsValues == null) return this;
            
            foreach (var tag in tagsValues)
            {
                this.tags[tag.Key] = tag.Value;
            }

            return this;
        }

        /// <summary>
        /// Publish the events
        /// </summary>
        public void Publish()
        {
            // Tags
            foreach(var ev in this.events)
            {
                ev.Tags = this.tags;
            }

            this.streamEventsProducer.Publish(this.events);
        }

    }

}
