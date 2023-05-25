using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Represents a single point in time with event value and tags attached to it
    /// </summary>
    public class EventData
    {
        /// <summary>
        /// Create a new empty Event Data instance
        /// </summary>
        internal EventData()
        {
            SetTags(new Dictionary<string, string>());
        }

        /// <summary>
        /// Create a new empty Event Data instance
        /// </summary>
        public EventData(string eventId, long timestampNanoseconds, string eventValue) : this()
        {
            this.EpochIncluded = false;
            this.TimestampNanoseconds = timestampNanoseconds;
            this.Id = eventId;
            this.Value = eventValue;
        }

        /// <summary>
        /// Create a new empty Event Data instance
        /// </summary>
        public EventData(string eventId, DateTime timestamp, string eventValue) : this(eventId, timestamp.ToUnixNanoseconds(), eventValue)
        {
            this.EpochIncluded = true;
        }

        /// <summary>
        /// Create a new empty Event Data instance
        /// </summary>
        public EventData(string eventId, TimeSpan timestamp, string eventValue) : this(eventId, timestamp.ToNanoseconds(), eventValue)
        {
            this.EpochIncluded = false;
        }

        /// <summary>
        /// Clones the <see cref="EventData"/>
        /// </summary>
        /// <returns>Cloned data</returns>
        public EventData Clone()
        {
            var data = new EventData();
            data.CopyFrom(this);

            return data;
        }

        /// <summary>
        /// Create a new Event Data instance loading data from <see cref="EventDataRaw"/> type instance
        /// </summary>
        /// <param name="rawData">Event Data to load from</param>
        internal EventData(QuixStreams.Telemetry.Models.EventDataRaw rawData)
        {
            this.LoadFromEventDataRaw(rawData);
        }

        internal void SetTags(IDictionary<string, string> newTags)
        {
            this.Tags = newTags;
        }

        private void LoadFromEventDataRaw(QuixStreams.Telemetry.Models.EventDataRaw rawData)
        {
            this.EpochIncluded = true;
            this.TimestampNanoseconds = rawData.Timestamp;
            this.Id = rawData.Id;
            this.Value = rawData.Value;
            this.SetTags(rawData.Tags.ToDictionary(kv => kv.Key, kv => kv.Value));

        }

        private void CopyFrom(Streaming.Models.EventData data)
        {
            this.EpochIncluded = data.EpochIncluded;
            this.TimestampNanoseconds = data.TimestampNanoseconds;
            this.Id = data.Id;
            this.Value = data.Value;
            this.SetTags(data.Tags.ToDictionary(kv => kv.Key, kv => kv.Value));
        }

        internal QuixStreams.Telemetry.Models.EventDataRaw ConvertToEventDataRaw()
        {
            return new QuixStreams.Telemetry.Models.EventDataRaw
            {
                Timestamp = this.TimestampNanoseconds,
                Id = this.Id,
                Value = this.Value,
                Tags = this.Tags.ToDictionary(kv => kv.Key, kv => kv.Value)
            };
        }

        /// <summary>
        /// The globally unique identifier of the event
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The value of the event
        /// </summary>
        public string Value { get; set; }


        /// <summary>
        /// Tags for the timestamp. If key is not found return null
        /// </summary>
        public IDictionary<string, string> Tags { get; private set; }

        /// <summary>
        /// Add a new Tag to the event
        /// </summary>
        /// <param name="tagId">Tag name</param>
        /// <param name="tagValue">Tag value</param>
        /// <returns>This instance</returns>
        public EventData AddTag(string tagId, string tagValue)
        {
            if (string.IsNullOrWhiteSpace(tagId)) throw new ArgumentNullException(nameof(tagId), "Tag id can't be null or empty");
            if (tagValue == null) return this.RemoveTag(tagId);
            
            this.Tags[tagId] = tagValue;

            return this;
        }
        
        /// <summary>
        /// Copies the tags from the specified dictionary.
        /// Conflicting tags will be overwritten
        /// </summary>
        /// <param name="tags">The tags to copy</param>
        /// <returns>This instance</returns>
        public EventData AddTags(IEnumerable<KeyValuePair<string, string>> tags)
        {
            if (tags == null) return this;

            foreach (var tagPair in tags)
            {
                this.AddTag(tagPair.Key, tagPair.Value);
            }

            return this;
        }

        /// <summary>
        /// Remove a Tag from the event
        /// </summary>
        /// <param name="tagId">Tag name</param>
        /// <returns>This instance</returns>
        public EventData RemoveTag(string tagId)
        {
            this.Tags.Remove(tagId);

            return this;
        }

        /// <summary>
        /// Gets the timestamp in nanoseconds
        /// </summary>
        public long TimestampNanoseconds { get; internal set; }

        /// <summary>
        /// Gets the timestamp in milliseconds
        /// </summary>
        public long TimestampMilliseconds => this.TimestampNanoseconds / (long) 1e6;

        /// <summary>
        /// Gets the timestamp in <see cref="DateTime"/> format
        /// </summary>
        public DateTime Timestamp => this.TimestampNanoseconds.FromUnixNanoseconds();

        /// <summary>
        /// Gets the timestamp in <see cref="TimeSpan"/> format
        /// </summary>
        public TimeSpan TimestampAsTimeSpan=> this.TimestampNanoseconds.FromNanoseconds();

        /// <summary>
        /// Epoch is included in the Timestamp values
        /// </summary>
        internal bool EpochIncluded { get; set; }
    }


}