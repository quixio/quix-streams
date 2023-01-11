﻿using Quix.Sdk.Process.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Builder for creating <see cref="EventData"/> packages within the <see cref="StreamEventsWriter"/> 
    /// </summary>
    public class EventDataBuilder
    {
        private readonly StreamEventsWriter streamEventsWriter;
        private readonly long timestampNanoseconds;

        private readonly List<Process.Models.EventDataRaw> events = new List<Process.Models.EventDataRaw>();
        private readonly Dictionary<string, string> tags;

        /// <summary>
        /// Initializes a new instance of <see cref="EventDataBuilder"/>
        /// </summary>
        /// <param name="streamEventsWriter">Events writer owner</param>
        /// <param name="timestampNanoseconds">Timestamp assigned to the Events created by the builder</param>
        public EventDataBuilder(StreamEventsWriter streamEventsWriter, long timestampNanoseconds)
        {
            this.streamEventsWriter = streamEventsWriter;
            this.timestampNanoseconds = timestampNanoseconds;
            this.tags = new Dictionary<string, string>(this.streamEventsWriter.DefaultTags);
        }

        /// <summary>
        /// Adds new event value at the time the builder is created for
        /// </summary>
        /// <param name="eventId">Event Id</param>
        /// <param name="value">Value of the event</param>
        /// <returns></returns>
        public EventDataBuilder AddValue(string eventId, string value)
        {
            var @event = new Process.Models.EventDataRaw()
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
        /// Writes the events
        /// </summary>
        public void Write()
        {
            // Tags
            foreach(var ev in this.events)
            {
                ev.Tags = this.tags;
            }

            this.streamEventsWriter.Write(this.events);
        }

    }

}
