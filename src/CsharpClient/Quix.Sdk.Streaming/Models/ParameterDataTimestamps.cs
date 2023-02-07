using Quix.Sdk.Streaming.Models;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Quix.Sdk.Streaming.Utils
{
    /// <summary>
    /// Enumerable which returns the Timestamps of the current <see cref="ParameterData"/>
    /// </summary>
    public readonly struct ParameterDataTimestamps : IReadOnlyCollection<ParameterDataTimestamp>
    {
        private readonly ParameterData parameterData;

        internal ParameterDataTimestamps(ParameterData parameterData)
        {
            this.parameterData = parameterData;
        }

        /// <inheritdoc/>
        public IEnumerator<ParameterDataTimestamp> GetEnumerator()
        {
            foreach (var timestamp in this.parameterData.timestampsList)
            {
                yield return new ParameterDataTimestamp(this.parameterData, timestamp);
            }
        }

        IEnumerator<ParameterDataTimestamp> IEnumerable<ParameterDataTimestamp>.GetEnumerator()
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
        public ParameterDataTimestamp this[int index]
        {
            get
            {
                return new ParameterDataTimestamp(this.parameterData, this.parameterData.timestampsList[index]);
            }
        }

        /// <summary>
        /// Number of Timestamps
        /// </summary>
        public int Count => this.parameterData.timestampsList.Count;

        internal void RemoveAt(int index)
        {
            this.parameterData.RemoveTimestamp(index);
        }
    }
}