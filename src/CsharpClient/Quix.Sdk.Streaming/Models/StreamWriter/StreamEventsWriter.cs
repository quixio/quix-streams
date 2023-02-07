using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Process.Models.Utility;
using Quix.Sdk.Process.Managers;
using System.Linq;
using Quix.Sdk.Streaming.Exceptions;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Helper class for writing <see cref="EventDefinitions"/> and <see cref="EventData"/>
    /// </summary>
    public class StreamEventsWriter : IDisposable
    {
        private readonly ILogger logger = Logging.CreateLogger<StreamEventsWriter>();
        private readonly IStreamWriterInternal streamWriter;

        private long epoch = 0;

        private string location;
        private readonly EventDefinitionsManager eventDefinitionsManager = new EventDefinitionsManager();
        private readonly Timer flushDefinitionsTimer;
        private bool timerEnabled = false; // Here because every now and then reseting its due time to never doesn't work
        private const int TimerInterval = 200;
        private readonly object flushLock = new object();
        private bool isDisposed;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamEventsWriter"/>
        /// </summary>
        /// <param name="streamWriter">Stream writer owner</param>
        internal StreamEventsWriter(IStreamWriterInternal streamWriter)
        {
            this.streamWriter = streamWriter;

            // Timer for Flush Parameter definitions
            flushDefinitionsTimer = new Timer(OnFlushDefinitionsTimerEvent, null, Timeout.Infinite, Timeout.Infinite);

            // Initialize root location
            this.DefaultLocation = "/";
        }

        /// <summary>
        /// Default Tags injected to all Event Values sent by the writer.
        /// </summary>
        public Dictionary<string, string> DefaultTags { get; set; } = new Dictionary<string, string>();

        /// <summary>
        /// Default Location of the events. Event definitions added with <see cref="AddDefinition"/> will be inserted at this location.
        /// See <see cref="AddLocation"/> for adding definitions at a different location without changing default.
        /// Example: "/Group1/SubGroup2"
        /// </summary>
        public string DefaultLocation
        {
            get
            {
                return this.location;
            }
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamEventsWriter));
                }
                this.location = this.eventDefinitionsManager.ReformatLocation(value);
            }
        }

        /// <summary>
        /// Default Epoch used for Timestamp event values. Datetime added on top of all the Timestamps.
        /// </summary>
        public DateTime Epoch
        {
            get
            {
                return epoch.FromUnixNanoseconds();
            }
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamEventsWriter));
                }
                epoch = value.ToUnixNanoseconds();
            }
        }

        /// <summary>
        /// Starts adding a new set of event values at the given timestamp.
        /// Note, <see cref="Epoch"/> is not used when invoking with <see cref="DateTime"/>
        /// </summary>
        /// <param name="dateTime">The datetime to use for adding new event values</param>
        /// <returns>Event data builder to add event values at the provided time</returns>
        public EventDataBuilder AddTimestamp(DateTime dateTime) => this.AddTimestampNanoseconds(dateTime.ToUnixNanoseconds(), 0);

        /// <summary>
        /// Starts adding a new set of event values at the given timestamp.
        /// </summary>
        /// <param name="timeSpan">The time since the default <see cref="Epoch"/> to add the event values at</param>
        /// <returns>Event data builder to add event values at the provided time</returns>
        public EventDataBuilder AddTimestamp(TimeSpan timeSpan) => this.AddTimestampNanoseconds(timeSpan.ToNanoseconds());

        /// <summary>
        /// Starts adding a new set of event values at the given timestamp.
        /// </summary>
        /// <param name="timeMilliseconds">The time in milliseconds since the default <see cref="Epoch"/> to add the event values at</param>
        /// <returns>Event data builder to add event values at the provided time</returns>
        public EventDataBuilder AddTimestampMilliseconds(long timeMilliseconds) => this.AddTimestampNanoseconds(timeMilliseconds * (long) 1e6);

        /// <summary>
        /// Starts adding a new set of event values at the given timestamp.
        /// </summary>
        /// <param name="timeNanoseconds">The time in nanoseconds since the default <see cref="Epoch"/> to add the event values at</param>
        /// <returns>Event data builder to add event values at the provided time</returns>
        public EventDataBuilder AddTimestampNanoseconds(long timeNanoseconds)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            return AddTimestampNanoseconds(timeNanoseconds, this.epoch);
        }
        
        private EventDataBuilder AddTimestampNanoseconds(long timeNanoseconds, long epoch)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            return new EventDataBuilder(this, epoch + timeNanoseconds);
        }

        /// <summary>
        /// Adds a list of definitions to the <see cref="StreamEventsWriter"/>. Configure it with the builder methods.
        /// </summary>
        /// <param name="definitions">List of definitions</param>
        public void AddDefinitions(List<EventDefinition> definitions)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            definitions.ForEach(d => this.eventDefinitionsManager.AddDefinition(d.ConvertToProcessDefinition(), d.Location));

            this.ResetFlushDefinitionsTimer();
        }

        /// <summary>
        /// Adds new Event definition, to define properties like Name or Level, among others.
        /// </summary>
        /// <param name="eventId">Event Id. This must match the event id you use to Event values</param>
        /// <param name="name">Human friendly display name of the event</param>
        /// <param name="description">Description of the event</param>
        /// <returns>Event definition builder to define the event properties</returns>
        public EventDefinitionBuilder AddDefinition(string eventId, string name = null, string description = null)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            var eventDefinition = this.CreateDefinition(this.location, eventId, name, description);

            var builder = new EventDefinitionBuilder(this, this.location, eventDefinition);

            return builder;
        }

        /// <summary>
        /// Adds a new Location in the event groups hierarchy.
        /// </summary>
        /// <param name="location"></param>
        public EventDefinitionBuilder AddLocation(string location)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            this.eventDefinitionsManager.GenerateLocations(location);

            var builder = new EventDefinitionBuilder(this, location);

            return builder;
        }

        internal Process.Models.EventDefinition CreateDefinition(string location, string eventId, string name, string description)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            var eventDefinition = new Process.Models.EventDefinition
            {
                Id = eventId,
                Name = name,
                Description = description
            };

            eventDefinitionsManager.AddDefinition(eventDefinition, location);

            this.ResetFlushDefinitionsTimer();

            return eventDefinition;
        }


        /// <summary>
        /// Immediately writes the event definitions from the buffer without waiting for buffer condition to fulfill (200ms timeout)
        /// </summary>
        public void Flush()
        {
            this.Flush(false);
        }

        private void Flush(bool force)
        {
            if (!force && isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            try
            {
                lock (flushLock)
                {
                    this.FlushDefinitions();
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Exception occurred while trying to flush events data buffer.");
            }
        }

        /// <summary>
        /// Write event into the stream
        /// </summary>
        /// <param name="data">Event data to write</param>
        public void Write(EventData data)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            if (!data.EpochIncluded)
            {
                data.TimestampNanoseconds += this.Epoch.ToUnixNanoseconds();
                data.EpochIncluded = true;
            }

            foreach (var kv in this.DefaultTags)
            {
                if (!data.Tags.ContainsKey(kv.Key))
                {
                    data.AddTag(kv.Key, kv.Value);
                }
            }

            this.streamWriter.Write(data.ConvertToProcessData());
            this.logger.Log(LogLevel.Trace, "event '{0}' sent.", data.Id);
        }

        /// <summary>
        /// Write events into the stream
        /// </summary>
        /// <param name="events">Events to write</param>
        public void Write(ICollection<EventData> events)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            foreach(var data in events)
            {
                foreach (var kv in this.DefaultTags)
                {
                    if (!data.Tags.ContainsKey(kv.Key))
                    {
                        data.AddTag(kv.Key, kv.Value);
                    }
                }
            }

            var batch = events.Select(e => e.ConvertToProcessData()).ToArray();

            this.streamWriter.Write(batch);
            this.logger.Log(LogLevel.Trace, "{0} event(s) sent.", events.Count);
        }

        internal void Write(ICollection<Process.Models.EventDataRaw> events)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsWriter));
            }
            this.streamWriter.Write(events);
        }

        private void ResetFlushDefinitionsTimer()
        {
            if (isDisposed) return;
            timerEnabled = true;
            flushDefinitionsTimer.Change(TimerInterval, Timeout.Infinite);
        }

        private void OnFlushDefinitionsTimerEvent(object state)
        {
            if (!timerEnabled) return;
            try
            {
                this.FlushDefinitions();
            }
            catch (StreamClosedException exception) when (this.isDisposed)
            {
                // Ignore exception because the timer flush definition may finish executing only after closure due to how close lock works in streamWriter
            }
            catch (Exception ex)
            {
                this.logger.Log(LogLevel.Error, ex, "Exception occurred while trying to flush event definition buffer.");
            }
        }


        private void FlushDefinitions()
        {
            timerEnabled = false;
            flushDefinitionsTimer.Change(Timeout.Infinite, Timeout.Infinite);

            var definitions = eventDefinitionsManager.GenerateEventDefinitions();
            
            if (definitions.Events?.Count == 0 && definitions.EventGroups?.Count == 0) return; // there is nothing to flush

            this.streamWriter.Write(definitions);
        }

        /// <summary>
        /// Flushes internal buffers and disposes
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed) return;
            this.isDisposed = true;
            this.Flush(true);
            flushDefinitionsTimer?.Dispose();
        }
    }
}
