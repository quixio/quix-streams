using System;
using System.Collections.Generic;
using QuixStreams.Streaming.Models.StreamProducer;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Leading edge buffer where time is the only key and tags are not treated as part of the key
    /// </summary>
    public class LeadingEdgeTimeBuffer
    {
        
        /// <summary>
        /// Sorted dictionary of rows in the buffer
        /// </summary>
        private readonly SortedDictionary<long, LeadingEdgeTimeRow> rows;

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
        internal LeadingEdgeTimeBuffer(StreamTimeseriesProducer producer, int leadingEdgeDelayMs)
        {
            this.producer = producer;
            this.leadingEdgeDelayInNanoseconds = leadingEdgeDelayMs * (long)1e6;
            this.rows = new SortedDictionary<long, LeadingEdgeTimeRow>();
        }


        /// <summary>
        /// Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist.
        /// </summary>
        /// <param name="timestampInNanoseconds">Timestamp in nanoseconds</param>
        /// <returns></returns>
        public LeadingEdgeTimeRow GetOrCreateTimestamp(long timestampInNanoseconds)
        {
            var ts = timestampInNanoseconds + (this.Epoch ?? 0);
            if (!rows.TryGetValue(ts, out var row))
            {
                row = new LeadingEdgeTimeRow(ts,this.Epoch != null, null);
                this.rows.Add(ts, row);
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
            
            var itemsToPublish = new List<KeyValuePair<long, LeadingEdgeTimeRow>>(rows.Count); // prep for worst case to avoid constant resizing
            var itemsToBackFill = new List<KeyValuePair<long, LeadingEdgeTimeRow>>(rows.Count); // prep for worst case to avoid constant resizing

            foreach (var row in rows)
            {
                var tsInNanoseconds = row.Key;

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
        
        private void RaiseBackfillData(List<KeyValuePair<long, LeadingEdgeTimeRow>> itemsToBackFill)
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

        private void PublishLeadingEdgeData(List<KeyValuePair<long, LeadingEdgeTimeRow>> itemsToPublish)
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
    }
    
    
    /// <summary>
    /// Represents a single row of data in the <see cref="LeadingEdgeTimeBuffer"/>
    /// </summary>
    public class LeadingEdgeTimeRow : LeadingEdgeRow
    {
        /// <summary>
        /// Adds a tag to the event values
        /// </summary>
        /// <param name="tagId">Tag Id</param>
        /// <param name="value">Tag value</param>
        /// <param name="overwrite">Whether to overwrite existing value</param>
        public LeadingEdgeTimeRow AddTag(string tagId, string value, bool overwrite = false)
        {
            this.Tags ??= new Dictionary<string, string>();
            if (overwrite || !this.Tags.ContainsKey(tagId))
            {
                this.Tags[tagId] = value;
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
        public new LeadingEdgeTimeRow AddValue(string parameter, double value, bool overwrite = false)
        {
            return (LeadingEdgeTimeRow)base.AddValue(parameter, value, overwrite);

        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.</param>
        /// <returns>Returns the current LeadingEdgeTimestamp instance. This allows for method chaining</returns>
        public new LeadingEdgeTimeRow AddValue(string parameter, string value, bool overwrite = false)
        {
            return (LeadingEdgeTimeRow)base.AddValue(parameter, value, overwrite);
        }

        /// <summary>
        /// Adds a value to the row
        /// </summary>
        /// <param name="parameter">Parameter name</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="overwrite">If set to true, it will overwrite an existing value for the specified parameter if one already exists.
        /// If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current LeadingEdgeTimestamp instance.</param>
        public new LeadingEdgeTimeRow AddValue(string parameter, byte[] value, bool overwrite = false)
        {
            return (LeadingEdgeTimeRow)base.AddValue(parameter, value, overwrite);
        }

        internal LeadingEdgeTimeRow(long timestamp, bool epochIncluded, Dictionary<string, string> tags) : base(timestamp, epochIncluded, tags)
        {
        }
    }
}