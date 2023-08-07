using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using QuixStreams;
using QuixStreams.Streaming.Exceptions;
using QuixStreams.Telemetry.Managers;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// Helper class for producing <see cref="EventDefinitions"/> and <see cref="EventData"/>
    /// </summary>
    public class StreamEventsProducer : IDisposable
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<StreamEventsProducer>();
        private readonly IStreamProducerInternal streamProducer;

        private long epoch = 0;

        private string location;
        private readonly EventDefinitionsManager eventDefinitionsManager = new EventDefinitionsManager();
        private readonly Timer flushDefinitionsTimer;
        private bool timerEnabled = false; // Here because every now and then resetting its due time to never doesn't work
        private const int TimerInterval = 200;
        private readonly object flushLock = new object();
        private bool isDisposed;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamEventsProducer"/>
        /// </summary>
        /// <param name="streamProducer">The Stream producer which owns this stream events producer</param>
        internal StreamEventsProducer(IStreamProducerInternal streamProducer)
        {
            this.streamProducer = streamProducer;

            // Timer for Flush Parameter definitions
            flushDefinitionsTimer = new Timer(OnFlushDefinitionsTimerEvent, null, Timeout.Infinite, Timeout.Infinite);

            // Initialize root location
            this.DefaultLocation = "/";
        }

        /// <summary>
        /// Default Tags injected to all Event Values sent by the producer.
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
                    throw new ObjectDisposedException(nameof(StreamEventsProducer));
                }
                this.location = this.eventDefinitionsManager.ReformatLocation(value);
            }
        }

        /// <summary>
        /// Default epoch used for Timestamp event values. Datetime added on top of all the Timestamps.
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
                    throw new ObjectDisposedException(nameof(StreamEventsProducer));
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
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            return AddTimestampNanoseconds(timeNanoseconds, this.epoch);
        }
        
        private EventDataBuilder AddTimestampNanoseconds(long timeNanoseconds, long epoch)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            return new EventDataBuilder(this, epoch + timeNanoseconds);
        }

        /// <summary>
        /// Adds a list of definitions to the <see cref="StreamEventsProducer"/>. Configure it with the builder methods.
        /// </summary>
        /// <param name="definitions">List of definitions</param>
        public void AddDefinitions(List<EventDefinition> definitions)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            definitions.ForEach(d => this.eventDefinitionsManager.AddDefinition(d.ConvertToTelemetryDefinition(), d.Location));

            this.ResetFlushDefinitionsTimer();
        }

        /// <summary>
        /// Add new Event definition to define properties like Name or Level, among others.
        /// </summary>
        /// <param name="eventId">Event Id. This must match the event id you use to Event values</param>
        /// <param name="name">Human friendly display name of the event</param>
        /// <param name="description">Description of the event</param>
        /// <returns>Event definition builder to define the event properties</returns>
        public EventDefinitionBuilder AddDefinition(string eventId, string name = null, string description = null)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            var eventDefinition = this.CreateDefinition(this.location, eventId, name, description);

            var builder = new EventDefinitionBuilder(this, this.location, eventDefinition);

            return builder;
        }

        /// <summary>
        /// Adds a new Location in the event groups hierarchy.
        /// </summary>
        /// <param name="location">The group location</param>
        public EventDefinitionBuilder AddLocation(string location)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            this.eventDefinitionsManager.GenerateLocations(location);

            var builder = new EventDefinitionBuilder(this, location);

            return builder;
        }

        internal QuixStreams.Telemetry.Models.EventDefinition CreateDefinition(string location, string eventId, string name, string description)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            var eventDefinition = new QuixStreams.Telemetry.Models.EventDefinition
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
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
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
        /// Publish an event into the stream.
        /// </summary>
        /// <param name="data">Event to publish</param>
        public void Publish(EventData data)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
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

            this.streamProducer.Publish(data.ConvertToEventDataRaw());
            this.logger.Log(LogLevel.Trace, "event '{0}' sent.", data.Id);
        }

        /// <summary>
        /// Publish events into the stream.
        /// </summary>
        /// <param name="events">Events to publish</param>
        public void Publish(ICollection<EventData> events)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
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

            var batch = events.Select(e => e.ConvertToEventDataRaw()).ToArray();

            this.streamProducer.Publish(batch);
            this.logger.Log(LogLevel.Trace, "{0} event(s) sent.", events.Count);
        }

        internal void Publish(ICollection<QuixStreams.Telemetry.Models.EventDataRaw> events)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamEventsProducer));
            }
            this.streamProducer.Publish(events);
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
                // Ignore exception because the timer flush definition may finish executing only after closure due to how close lock works in streamProducer
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

            this.streamProducer.Publish(definitions);
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
