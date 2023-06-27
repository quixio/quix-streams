using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Streaming.Models.StreamConsumer;
using QuixStreams.Streaming.Models.StreamProducer;

namespace QuixStreams.Streaming.Models
{
    public class LeadingEdgeBuffer
    {
        /// <summary>
        /// Sorted dictionary of rows in the buffer. Key is a tuple of the timestamp and a hash of the tags. 
        /// </summary>
        private readonly SortedDictionary<(long, long), LeadingEdgeRow> rows;
        private readonly StreamTimeseriesProducer producer;
        private readonly long leadingEdgeDelayInNanoseconds;
        private readonly long bucketInNanoseconds;

        private long leadingEdgeInNanoseconds = Int64.MinValue;
        private long lastTimestampReleased = Int64.MinValue;

        /// <summary>
        /// Data arriving with a timestamp earlier then the lastest released timestamp is discarded but released in this event for further processing or forwarding.
        /// This event is invoked only if the buffer is configured with a leading edge delay.
        /// </summary>
        public event EventHandler<TimeseriesDataReadEventArgs> OnBackfill;
        
        /// <summary>
        /// Initializes a new instance of <see cref="LeadingEdgeBuffer"/>
        /// </summary>
        /// <param name="producer">Instance of <see cref="StreamTimeseriesProducer"/></param>
        /// <param name="leadingEdgeDelayMs">Leading edge delay configuration in Milliseconds</param>
        /// <param name="bucketMs">Time between timestamps to wait for before publishing data in milliseconds</param>
        public LeadingEdgeBuffer(StreamTimeseriesProducer producer, int leadingEdgeDelayMs, int bucketMs)
        {
            this.producer = producer;
            this.leadingEdgeDelayInNanoseconds = leadingEdgeDelayMs * (long)1e6;
            this.bucketInNanoseconds = bucketMs * (long)1e6;
            this.rows = new SortedDictionary<(long, long), LeadingEdgeRow>();
        }
        
        
        /// <summary>
        /// Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist.
        /// </summary>
        /// <param name="timestampInNanoseconds">Timestamp in nanoseconds</param>
        /// <param name="tags">Optional Tags</param>
        /// <returns></returns>
        public LeadingEdgeRow GetOrCreateTimestamp(long timestampInNanoseconds, Dictionary<string, string> tags = null)
        {
            if (!rows.TryGetValue((timestampInNanoseconds, TagsHash(tags)), out var row))
            {
                row = new LeadingEdgeRow(this, timestampInNanoseconds, tags);
            }

            return row;
        }

        internal void AddRowToBuffer(LeadingEdgeRow row)
        {
            if (rows.ContainsKey((row.Timestamp, TagsHash(row.Tags))))
            {
                return;
            }

            if (row.Timestamp <= this.lastTimestampReleased)
            {
                this.InvokeOnBackill(this, new TimeseriesDataReadEventArgs(null, null, row.AppendToTimeseriesData(new TimeseriesData())));
                return;
            }

            rows[(row.Timestamp, TagsHash(row.Tags))] = row;
            leadingEdgeInNanoseconds = Math.Max(this.leadingEdgeInNanoseconds, row.Timestamp);

            EnsurePublished();
        }

        /// <summary>
        /// Publishes data inside the buffer that should be published based on the leading edge delay and the bucket size configuration.
        /// </summary>
        private void EnsurePublished()
        {
            var keysToPublish = new List<(long, long)>();
            foreach (var row in rows)
            {
                var tsInNanoseconds = row.Key.Item1;

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
                var nano = keysToPublish[i].Item1;

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

        private void Publish(params (long, long)[] keys)
        {
            var timeseriesData = new TimeseriesData(capacity: keys.Length);
            foreach (var key in keys)
            {
                var timestampInNanoseconds = key.Item1;
                var row = rows[key];

                timeseriesData = row.AppendToTimeseriesData(timeseriesData);

                this.lastTimestampReleased = Math.Max(this.lastTimestampReleased, timestampInNanoseconds);
                rows.Remove(key);
            }

            producer.Publish(timeseriesData);
        }

        private void InvokeOnBackill(object sender, TimeseriesDataReadEventArgs args)
        {
            this.OnBackfill?.Invoke(this, args);
        }

        private static long TagsHash(Dictionary<string, string> tags)
        {
            if (tags == null || tags.Count == 0) return 0;
            unchecked
            {
                var hash = 397;
                foreach (var kpair in tags)
                {
                    hash ^= kpair.Value?.GetHashCode() ?? 0;
                    hash ^= kpair.Key.GetHashCode();
                }

                return hash;
            }
        }
    }

    /// <summary>
    /// Represents a single row of data in the <see cref="LeadingEdgeBuffer"/>
    /// </summary>
    public class LeadingEdgeRow
    {
        internal long Timestamp { get; }
        internal Dictionary<string, double> NumericValues { get; }
        internal Dictionary<string, string> StringValues { get; }
        internal Dictionary<string, byte[]> BinaryValues { get; }
        internal Dictionary<string, string> Tags { get; }

        private readonly LeadingEdgeBuffer buffer;
        private bool publishedToBuffer = false;

        public LeadingEdgeRow(LeadingEdgeBuffer buffer, long timestamp, Dictionary<string, string> tags)
        {
            this.buffer = buffer;
            this.Timestamp = timestamp;
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
        public LeadingEdgeRow AddValue(string parameter, double value, bool overwrite = false)
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
        public LeadingEdgeRow AddValue(string parameter, string value, bool overwrite = false)
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
        public LeadingEdgeRow AddValue(string parameter, byte[] value, bool overwrite = false)
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
            var ts = existingTimeseriesData.AddTimestampNanoseconds(Timestamp, epochIncluded: true).AddTags(Tags);
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