using System;
using System.Collections.Generic;
using System.Diagnostics;
using QuixStreams.Streaming.Models.StreamProducer;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Leading edge buffer where time and tag are treated as a compound key
    /// </summary>
    public class LeadingEdgeBuffer
    {
        
        /// <summary>
        /// Sorted dictionary of rows in the buffer
        /// </summary>
        private readonly SortedDictionary<LeadingEdgeRowKey, LeadingEdgeRow> rows;

        private readonly StreamTimeseriesProducer producer;
        private readonly long leadingEdgeDelayInNanoseconds;

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
        internal LeadingEdgeBuffer(StreamTimeseriesProducer producer, int leadingEdgeDelayMs)
        {
            this.producer = producer;
            this.leadingEdgeDelayInNanoseconds = leadingEdgeDelayMs * (long)1e6;
            this.rows = new SortedDictionary<LeadingEdgeRowKey, LeadingEdgeRow>();
        }


        /// <summary>
        /// Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist.
        /// </summary>
        /// <param name="timestampInNanoseconds">Timestamp in nanoseconds</param>
        /// <param name="tags">Optional Tags</param>
        /// <returns></returns>
        public LeadingEdgeRow GetOrCreateTimestamp(long timestampInNanoseconds, Dictionary<string, string> tags = null)
        {
            var ts = timestampInNanoseconds + (this.Epoch ?? 0);
            var key = new LeadingEdgeRowKey(ts, tags);
            if (!rows.TryGetValue(key, out var row))
            {
                row = new LeadingEdgeRow(ts, this.Epoch != null, tags);
                this.rows.Add(key, row);
                leadingEdgeInNanoseconds = Math.Max(this.leadingEdgeInNanoseconds, ts);
            }

            return row;
        }

        /// <summary>
        /// Publishes all data in the buffer, regardless of leading edge condition.
        /// </summary>
        public void Flush()
        {
            Release(true);
        }

        /// <summary>
        /// Publishes data according to leading edge condition
        /// </summary>
        public void Publish()
        {
            Release(false);
        }
        
        private void Release(bool flushAll)
        {
            if (rows.Count == 0) return;
            
            var itemsToPublish = new List<KeyValuePair<LeadingEdgeRowKey, LeadingEdgeRow>>(rows.Count); // prep for worst case to avoid constant resizing
            var itemsToBackFill = new List<KeyValuePair<LeadingEdgeRowKey, LeadingEdgeRow>>(rows.Count); // prep for worst case to avoid constant resizing

            foreach (var row in rows)
            {
                var tsInNanoseconds = row.Key.Timestamp;

                if (tsInNanoseconds <= this.lastTimestampReleased)
                {
                    itemsToBackFill.Add(row);
                    continue;
                }
                    
                if (flushAll || this.leadingEdgeInNanoseconds - tsInNanoseconds >= this.leadingEdgeDelayInNanoseconds)
                {
                    itemsToPublish.Add(row);
                    continue;
                }

                // Due to ordered nature of the dictionary, if we reach
                // to any we don't publish, we don't publish the rest either
                break; 
            }
            
            RaiseBackfillData(itemsToBackFill);

            PublishLeadingEdgeData(itemsToPublish);
        }
        
        private void RaiseBackfillData(List<KeyValuePair<LeadingEdgeRowKey, LeadingEdgeRow>> itemsToBackFill)
        {
            if (itemsToBackFill.Count == 0) return;

            var timeseriesData = new TimeseriesData(capacity: itemsToBackFill.Count);
            foreach (var item in itemsToBackFill)
            {
                rows.Remove(item.Key);
                timeseriesData = item.Value.AppendToTimeseriesData(timeseriesData);
            }

            this.OnBackfill?.Invoke(this, timeseriesData);
        }

        private void PublishLeadingEdgeData(List<KeyValuePair<LeadingEdgeRowKey, LeadingEdgeRow>> itemsToPublish)
        {
            if (itemsToPublish.Count == 0) return;

            var timeseriesData = new TimeseriesData(capacity: itemsToPublish.Count);
            foreach (var item in itemsToPublish)
            {
                rows.Remove(item.Key);
                timeseriesData = item.Value.AppendToTimeseriesData(timeseriesData);
            }
            
            this.lastTimestampReleased = Math.Max(this.lastTimestampReleased, timeseriesData.Timestamps[timeseriesData.Timestamps.Count-1].TimestampNanoseconds);

            this.OnPublish?.Invoke(this, timeseriesData);
            producer.Publish(timeseriesData);
        }

        /// <summary>
        /// Represents a compound key composed of a timestamp and a hash of a set of tags.
        /// Implements IComparable and IEquatable interfaces for comparison functionality.
        /// </summary>
        [DebuggerDisplay("{Timestamp}:{TagHash}")]
        private class LeadingEdgeRowKey : IComparable<LeadingEdgeRowKey>, IEquatable<LeadingEdgeRowKey>
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
            public LeadingEdgeRowKey(long timestamp, IDictionary<string, string> tags)
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
            public int CompareTo(LeadingEdgeRowKey other)
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
            public bool Equals(LeadingEdgeRowKey other)
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
    public class LeadingEdgeRow
    {
        /// <summary>
        /// The timestamp in nanoseconds
        /// </summary>
        public long Timestamp { get; }
        
         
        /// <summary>
        /// The numeric values of the timestamp
        /// </summary>
        internal Dictionary<string, double> NumericValues { get; private set; }
        
         
        /// <summary>
        /// The string values of the timestamp
        /// </summary>
        internal Dictionary<string, string> StringValues { get; private set; }
        
        /// <summary>
        /// The binary values of the timestamp
        /// </summary>
        internal Dictionary<string, byte[]> BinaryValues { get; private set; }
        
        /// <summary>
        /// This can't be edited externally, as it would mess the sorted dictionary implementation
        /// </summary>
        internal Dictionary<string, string> Tags { get; set; }
        
        /// <summary>
        /// Whether an epoch is included in the timestamp
        /// </summary>
        public readonly bool EpochIncluded = false;

        internal LeadingEdgeRow(long timestamp, bool epochIncluded, Dictionary<string, string> tags)
        {
            this.Timestamp = timestamp;
            this.EpochIncluded = epochIncluded;
            this.Tags = tags;
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.</param>
        /// <returns>Returns the current LeadingEdgeTimestamp instance. This allows for method chaining</returns>
        public LeadingEdgeRow AddValue(string parameter, double value, bool overwrite = false)
        {
            NumericValues ??= new Dictionary<string, double>();
            if (overwrite || !NumericValues.ContainsKey(parameter))
            {
                NumericValues[parameter] = value;
            }

            return this;
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.</param>
        /// <returns>Returns the current LeadingEdgeTimestamp instance. This allows for method chaining</returns>
        public LeadingEdgeRow AddValue(string parameter, string value, bool overwrite = false)
        {
            StringValues ??= new Dictionary<string, string>();
            if (overwrite || !StringValues.ContainsKey(parameter))
            {
                StringValues[parameter] = value;
            }
            
            return this;
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current LeadingEdgeTimestamp instance.</param>
        public LeadingEdgeRow AddValue(string parameter, byte[] value, bool overwrite = false)
        {
            BinaryValues ??= new Dictionary<string, byte[]>();
            if (overwrite || !BinaryValues.ContainsKey(parameter))
            {
                BinaryValues[parameter] = value;
            }

            return this;
        }

        internal TimeseriesData AppendToTimeseriesData(TimeseriesData existingTimeseriesData)
        {
            if (NumericValues == null && StringValues == null && BinaryValues == null) return existingTimeseriesData;
            var ts = existingTimeseriesData.AddTimestampNanoseconds(Timestamp, epochIncluded: EpochIncluded).AddTags(Tags);

            if (NumericValues != null)
            {
                foreach (var numericValue in NumericValues)
                {
                    ts.AddValue(numericValue.Key, numericValue.Value);
                }
            }

            if (StringValues != null)
            {
                foreach (var stringValue in StringValues)
                {
                    ts.AddValue(stringValue.Key, stringValue.Value);
                }
            }

            if (BinaryValues != null)
            {
                foreach (var binaryValue in BinaryValues)
                {
                    ts.AddValue(binaryValue.Key, binaryValue.Value);
                }
            }

            return existingTimeseriesData;
        }
    }
}