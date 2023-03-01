using System.Collections;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Streaming.Models;

namespace QuixStreams.Streaming.Utils
{
    /// <summary>
    /// ReadOnlyDictionary which returns the Tags of the current Timestamp
    /// </summary>
    public readonly struct TimeseriesDataTimestampTags : IReadOnlyDictionary<string, string>
    {
        private readonly TimeseriesData TimeseriesData;
        private readonly long timestampRawIndex;

        internal TimeseriesDataTimestampTags(TimeseriesData TimeseriesData, long timestampRawIndex)
        {
            this.TimeseriesData = TimeseriesData;
            this.timestampRawIndex = timestampRawIndex;
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            var localTimestamp = this.timestampRawIndex;
            return this.TimeseriesData.rawData.TagValues
                .ToDictionary(kv => kv.Key, kv => kv.Value[localTimestamp])
                .Where(kv => kv.Value != null)
                .GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Retrieves a Tag value by its Tag key
        /// </summary>
        /// <param name="key">Key of the tag</param>
        /// <returns></returns>
        public readonly string this[string key]
        {
            get
            {
                if (!this.TimeseriesData.rawData.TagValues.TryGetValue(key, out var tagValues))
                {
                    return null;
                }

                return tagValues[this.timestampRawIndex];
            }
        }

        /// <inheritdoc/>
        public IEnumerable<string> Keys => this.Select(kv => kv.Key);

        /// <inheritdoc/>
        public IEnumerable<string> Values => this.Select(kv => kv.Value);

        /// <inheritdoc/>
        public int Count => this.Count();

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            return this.TimeseriesData.rawData.TagValues.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out string value)
        {
            if (!this.TimeseriesData.rawData.TagValues.TryGetValue(key, out var tagValues))
            {
                value = null;
                return false;
            }

            value = tagValues[this.timestampRawIndex];
            return true;
        }
    }
}