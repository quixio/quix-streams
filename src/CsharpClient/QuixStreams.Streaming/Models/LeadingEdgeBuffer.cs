using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using QuixStreams.Streaming.Models.StreamProducer;

namespace QuixStreams.Streaming.Models
{
    public class LeadingEdgeBuffer
    {
        /// <summary>
        /// Sorted dictionary of rows in the buffer. Key is a tuple of the timestamp and a hash of the tags. 
        /// </summary>
        private readonly SortedDictionary<TimestampKey, LeadingEdgeTimestamp> rows;

        private readonly StreamTimeseriesProducer producer;
        private readonly long leadingEdgeDelayInNanoseconds;
        private readonly long bucketInNanoseconds;

        private long leadingEdgeInNanoseconds = long.MinValue;
        private long lastTimestampReleased = long.MinValue;

        /// <summary>
        /// Data arriving with a timestamp earlier then the latest released timestamp is discarded but released in this event for further processing or forwarding.
        /// </summary>
        public event EventHandler<TimeseriesData> OnBackfill;

        /// <summary>
        /// Event raised when <see cref="LeadingEdgeBuffer"/> condition is met and before data is published to the underlying <see cref="StreamTimeseriesProducer"/>.
        /// </summary>
        public event EventHandler<TimeseriesData> OnPublish;

        /// <summary>
        /// The epoch each timestamp is measured from. If null, epoch (if any) of the underlying producer will be used.
        /// </summary>
        public long? Epoch { get; set; }

        /// <summary>
        /// Initializes a new instance of <see cref="LeadingEdgeBuffer"/>
        /// </summary>
        /// <param name="producer">Instance of <see cref="StreamTimeseriesProducer"/></param>
        /// <param name="leadingEdgeDelayMs">Leading edge delay configuration in Milliseconds</param>
        /// <param name="bucketMs">Time between timestamps to wait for before publishing data in milliseconds</param>
        internal LeadingEdgeBuffer(StreamTimeseriesProducer producer, int leadingEdgeDelayMs, int bucketMs)
        {
            this.producer = producer;
            this.leadingEdgeDelayInNanoseconds = leadingEdgeDelayMs * (long)1e6;
            this.bucketInNanoseconds = bucketMs * (long)1e6;
            this.rows = new SortedDictionary<TimestampKey, LeadingEdgeTimestamp>();
        }


        /// <summary>
        /// Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist.
        /// </summary>
        /// <param name="timestampInNanoseconds">Timestamp in nanoseconds</param>
        /// <param name="tags">Optional Tags</param>
        /// <returns></returns>
        public LeadingEdgeTimestamp GetOrCreateTimestamp(long timestampInNanoseconds, Dictionary<string, string> tags = null)
        {
            var ts = timestampInNanoseconds + (this.Epoch ?? 0);
            var key = new TimestampKey(ts, tags);
            if (!rows.TryGetValue(key, out var row))
            {
                row = new LeadingEdgeTimestamp(this, ts, this.Epoch != null, tags);
            }

            return row;
        }

        internal void AddRowToBuffer(LeadingEdgeTimestamp timestamp)
        {
            var key = new TimestampKey(timestamp.Timestamp, timestamp.Tags);

            if (rows.ContainsKey(key))
            {
                return;
            }

            if (timestamp.Timestamp <= this.lastTimestampReleased)
            {
                this.OnBackfill?.Invoke(this, timestamp.AppendToTimeseriesData(new TimeseriesData()));
                return;
            }

            rows[key] = timestamp;
            leadingEdgeInNanoseconds = Math.Max(this.leadingEdgeInNanoseconds, timestamp.Timestamp);

            EnsurePublished();
        }

        /// <summary>
        /// Publishes data inside the buffer that should be published based on the leading edge delay and the bucket size configuration.
        /// </summary>
        private void EnsurePublished()
        {
            var keysToPublish = new List<TimestampKey>();
            foreach (var row in rows)
            {
                var tsInNanoseconds = row.Key.Timestamp;

                if (this.leadingEdgeInNanoseconds - tsInNanoseconds >= this.leadingEdgeDelayInNanoseconds)
                {
                    keysToPublish.Add(row.Key);
                }
            }

            int startIndex = 0;
            long minTimestamp = Int64.MaxValue;
            long maxTimestamp = Int64.MinValue;
            for (int i = 0; i < keysToPublish.Count; i++)
            {
                var nano = keysToPublish[i].Timestamp;

                minTimestamp = Math.Min(minTimestamp, nano);
                maxTimestamp = Math.Max(maxTimestamp, nano);

                var nsDiff = maxTimestamp - minTimestamp;
                if (nsDiff >= this.bucketInNanoseconds)
                {
                    // Reset min/max to this value, as will be flushing everything but this
                    minTimestamp = nano;
                    maxTimestamp = nano;

                    Publish(keysToPublish.GetRange(startIndex, i - startIndex).ToArray());

                    startIndex = i;
                }
            }
        }

        /// <summary>
        /// Flushes all data in the buffer
        /// </summary>
        public void Flush()
        {
            Publish(rows.Keys.ToArray());
        }

        private void Publish(params TimestampKey[] keys)
        {
            var timeseriesData = new TimeseriesData(capacity: keys.Length);
            foreach (var key in keys)
            {
                var timestampInNanoseconds = key.Timestamp;
                var row = rows[key];

                timeseriesData = row.AppendToTimeseriesData(timeseriesData);

                this.lastTimestampReleased = Math.Max(this.lastTimestampReleased, timestampInNanoseconds);
                rows.Remove(key);
            }

            producer.Publish(timeseriesData);
        }

        /// <summary>
        /// Represents a compound key composed of a timestamp and a hash of a set of tags.
        /// Implements IComparable and IEquatable interfaces for comparison functionality.
        /// </summary>
        [DebuggerDisplay("{Timestamp}:{TagHash}")]
        private class TimestampKey : IComparable<TimestampKey>, IEquatable<TimestampKey>
        {
            /// <summary>
            /// The timestamp part of the compound key.
            /// </summary>
            public long Timestamp { get; }

            /// <summary>
            /// The hash of the tags part of the compound key.
            /// </summary>
            private long TagHash { get; }

            /// <summary>
            /// Constructs a new instance of the TimestampKey class.
            /// </summary>
            /// <param name="timestamp">The timestamp.</param>
            /// <param name="tags">A dictionary of tags to generate the tag hash.</param>
            public TimestampKey(long timestamp, IDictionary<string, string> tags)
            {
                this.Timestamp = timestamp;
                this.TagHash = HashTags(tags);
            }

            /// <summary>
            /// Calculates a hash value from the given dictionary of tags.
            /// </summary>
            /// <param name="tags">A dictionary of tags to generate the hash.</param>
            /// <returns>The calculated hash.</returns>
            private static long HashTags(IDictionary<string, string> tags)
            {
                if (tags == null || tags.Count == 0) return 0;
                unchecked
                {
                    var hash = 397;
                    foreach (var kPair in tags)
                    {
                        hash ^= kPair.Value?.GetHashCode() ?? 0;
                        hash ^= kPair.Key.GetHashCode();
                    }

                    return hash;
                }
            }

            /// <summary>
            /// Compares the current instance with another instance of TimestampKey.
            /// </summary>
            /// <param name="other">The instance to compare with the current instance.</param>
            /// <returns>A value indicating the relative order of the instances.</returns>
            public int CompareTo(TimestampKey other)
            {
                if (other == null) return -1;
                var tsCompare = other.Timestamp.CompareTo(this.Timestamp);
                return tsCompare != 0 ? -tsCompare : -other.TagHash.CompareTo(this.TagHash);
            }

            /// <summary>
            /// Determines whether the current instance is equal to another instance of TimestampKey.
            /// </summary>
            /// <param name="other">An instance of TimestampKey to compare with the current instance.</param>
            /// <returns>True if the current instance is equal to the other instance; otherwise, false.</returns>
            public bool Equals(TimestampKey other)
            {
                if (other == null) return false;
                if (this.Timestamp != other.Timestamp) return false;
                return this.TagHash == other.TagHash;
            }
        }
    }


    /// <summary>
    /// Represents a single row of data in the <see cref="LeadingEdgeBuffer"/>
    /// </summary>
    public class LeadingEdgeTimestamp
    {
        internal long Timestamp { get; }
        internal Dictionary<string, double> NumericValues { get; }
        internal Dictionary<string, string> StringValues { get; }
        internal Dictionary<string, byte[]> BinaryValues { get; }
        internal Dictionary<string, string> Tags { get; }

        private readonly LeadingEdgeBuffer buffer;
        private bool publishedToBuffer = false;
        public readonly bool EpochIncluded = false;

        public LeadingEdgeTimestamp(LeadingEdgeBuffer buffer, long timestamp, bool epochIncluded, Dictionary<string, string> tags)
        {
            this.buffer = buffer;
            this.Timestamp = timestamp;
            this.EpochIncluded = epochIncluded;
            this.Tags = tags;

            NumericValues = new Dictionary<string, double>();
            StringValues = new Dictionary<string, string>();
            BinaryValues = new Dictionary<string, byte[]>();
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.</param>
        /// <returns>Returns the current TimeseriesDataRow instance. This allows for method chaining</returns>
        public LeadingEdgeTimestamp AddValue(string parameter, double value, bool overwrite = false)
        {
            if (NumericValues.ContainsKey(parameter) && !overwrite)
            {
                return this;
            }

            NumericValues[parameter] = value;
            return this;
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.</param>
        /// <returns>Returns the current TimeseriesDataRow instance. This allows for method chaining</returns>
        public LeadingEdgeTimestamp AddValue(string parameter, string value, bool overwrite = false)
        {
            if (StringValues.ContainsKey(parameter) && !overwrite)
            {
                return this;
            }

            StringValues[parameter] = value;
            return this;
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.</param>
        public LeadingEdgeTimestamp AddValue(string parameter, byte[] value, bool overwrite = false)
        {
            if (BinaryValues.ContainsKey(parameter) && !overwrite)
            {
                return this;
            }

            BinaryValues[parameter] = value;
            return this;
        }

        internal TimeseriesData AppendToTimeseriesData(TimeseriesData existingTimeseriesData)
        {
            var ts = existingTimeseriesData.AddTimestampNanoseconds(Timestamp, epochIncluded: EpochIncluded).AddTags(Tags);
            foreach (var numericValue in NumericValues)
            {
                ts.AddValue(numericValue.Key, numericValue.Value);
            }

            foreach (var stringValue in StringValues)
            {
                ts.AddValue(stringValue.Key, stringValue.Value);
            }

            foreach (var binaryValue in BinaryValues)
            {
                ts.AddValue(binaryValue.Key, binaryValue.Value);
            }

            return existingTimeseriesData;
        }
        
        /// <summary>
        /// This method commits the current object instance to the <see cref="LeadingEdgeBuffer"/>.
        /// </summary>
        public void Publish()
        {
            if (publishedToBuffer)
            {
                // Already published to the buffer
                return;
            }

            publishedToBuffer = true;
            buffer.AddRowToBuffer(this);
        }
    }
}