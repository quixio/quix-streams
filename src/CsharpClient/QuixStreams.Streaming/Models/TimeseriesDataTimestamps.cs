using System.Collections;
using System.Collections.Generic;
using QuixStreams.Streaming.Models;

namespace QuixStreams.Streaming.Utils
{
    /// <summary>
    /// Enumerable which returns the Timestamps of the current <see cref="TimeseriesData"/>
    /// </summary>
    public readonly struct TimeseriesDataTimestamps : IReadOnlyCollection<TimeseriesDataTimestamp>
    {
        private readonly TimeseriesData TimeseriesData;

        internal TimeseriesDataTimestamps(TimeseriesData TimeseriesData)
        {
            this.TimeseriesData = TimeseriesData;
        }

        /// <inheritdoc/>
        public IEnumerator<TimeseriesDataTimestamp> GetEnumerator()
        {
            foreach (var timestamp in this.TimeseriesData.timestampsList)
            {
                yield return new TimeseriesDataTimestamp(this.TimeseriesData, timestamp);
            }
        }

        IEnumerator<TimeseriesDataTimestamp> IEnumerable<TimeseriesDataTimestamp>.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Retrieve a Timestamp by index
        /// </summary>
        /// <param name="index">Index of the timestamp</param>
        /// <returns>Timestamp data at the given index</returns>
        public TimeseriesDataTimestamp this[int index]
        {
            get
            {
                return new TimeseriesDataTimestamp(this.TimeseriesData, this.TimeseriesData.timestampsList[index]);
            }
        }

        /// <summary>
        /// Number of Timestamps
        /// </summary>
        public int Count => this.TimeseriesData.timestampsList.Count;

        internal void RemoveAt(int index)
        {
            this.TimeseriesData.RemoveTimestamp(index);
        }
    }
}