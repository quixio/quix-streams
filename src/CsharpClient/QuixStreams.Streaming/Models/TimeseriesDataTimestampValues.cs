using System.Collections;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Streaming.Models;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Enumerable which returns the the Parameter Values of the current <see cref="TimeseriesDataTimestamp"/>
    /// </summary>
    public readonly struct TimeseriesDataTimestampValues : IReadOnlyDictionary<string, ParameterValue>
    {
        private readonly TimeseriesData TimeseriesData;
        private readonly long timestampRawIndex;

        internal TimeseriesDataTimestampValues(TimeseriesData TimeseriesData, long timestampRawIndex)
        {
            this.TimeseriesData = TimeseriesData;
            this.timestampRawIndex = timestampRawIndex;
        }

        readonly IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<string, ParameterValue>>)this).GetEnumerator();
        }

        readonly IEnumerator<KeyValuePair<string, ParameterValue>> IEnumerable<KeyValuePair<string, ParameterValue>>.GetEnumerator()
        {
            foreach (var parameter in this.TimeseriesData.parameterList.Values)
            {
                yield return new KeyValuePair<string, ParameterValue>(parameter.ParameterId, new ParameterValue(this.timestampRawIndex, parameter));
            }
        }

        /// <inheritdoc/>
        public readonly IEnumerable<ParameterValue> Values
        {
            get
            {
                foreach (var parameter in this.TimeseriesData.parameterList.Values)
                {
                    yield return new ParameterValue(this.timestampRawIndex, parameter);
                }
            }
        }

        /// <inheritdoc/>
        public readonly bool ContainsKey(string key)
        {
            return this.TimeseriesData.parameterList.ContainsKey(key);
        }

        /// <inheritdoc/>
        public readonly bool TryGetValue(string key, out ParameterValue value)
        {
            if (!this.TimeseriesData.parameterList.TryGetValue(key, out var valueOut))
            {
                value = default;
                return false;
            }

            value = new ParameterValue(this.timestampRawIndex, valueOut);
            return true;
        }


        /// <inheritdoc/>
        public readonly int Count => this.TimeseriesData.parameterList.Count();

        /// <inheritdoc/>
        public readonly IEnumerable<string> Keys => this.TimeseriesData.parameterList.Keys;

        /// <inheritdoc/>
        public readonly ParameterValue this[string key]
        {
            get
            {
                if (!this.TimeseriesData.parameterList.TryGetValue(key, out var parameter))
                {
                    parameter = new TimeseriesDataParameter(key);
                }

                return new ParameterValue(this.timestampRawIndex, parameter);
            }
        }

    }
}