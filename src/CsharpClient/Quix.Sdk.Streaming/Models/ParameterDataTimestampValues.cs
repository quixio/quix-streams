using Quix.Sdk.Streaming.Models;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Enumerable which returns the the Parameter Values of the current <see cref="ParameterDataTimestamp"/>
    /// </summary>
    public readonly struct ParameterDataTimestampValues : IReadOnlyDictionary<string, ParameterValue>
    {
        private readonly ParameterData parameterData;
        private readonly long timestampRawIndex;

        internal ParameterDataTimestampValues(ParameterData parameterData, long timestampRawIndex)
        {
            this.parameterData = parameterData;
            this.timestampRawIndex = timestampRawIndex;
        }

        readonly IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<string, ParameterValue>>)this).GetEnumerator();
        }

        readonly IEnumerator<KeyValuePair<string, ParameterValue>> IEnumerable<KeyValuePair<string, ParameterValue>>.GetEnumerator()
        {
            foreach (var parameter in this.parameterData.parameterList.Values)
            {
                yield return new KeyValuePair<string, ParameterValue>(parameter.ParameterId, new ParameterValue(this.timestampRawIndex, parameter));
            }
        }

        /// <inheritdoc/>
        public readonly IEnumerable<ParameterValue> Values
        {
            get
            {
                foreach (var parameter in this.parameterData.parameterList.Values)
                {
                    yield return new ParameterValue(this.timestampRawIndex, parameter);
                }
            }
        }

        /// <inheritdoc/>
        public readonly bool ContainsKey(string key)
        {
            return this.parameterData.parameterList.ContainsKey(key);
        }

        /// <inheritdoc/>
        public readonly bool TryGetValue(string key, out ParameterValue value)
        {
            if (!this.parameterData.parameterList.TryGetValue(key, out var valueOut))
            {
                value = default;
                return false;
            }

            value = new ParameterValue(this.timestampRawIndex, valueOut);
            return true;
        }


        /// <inheritdoc/>
        public readonly int Count => this.parameterData.parameterList.Count();

        /// <inheritdoc/>
        public readonly IEnumerable<string> Keys => this.parameterData.parameterList.Keys;

        /// <inheritdoc/>
        public readonly ParameterValue this[string key]
        {
            get
            {
                if (!this.parameterData.parameterList.TryGetValue(key, out var parameter))
                {
                    parameter = new Parameter(key);
                }

                return new ParameterValue(this.timestampRawIndex, parameter);
            }
        }

    }
}