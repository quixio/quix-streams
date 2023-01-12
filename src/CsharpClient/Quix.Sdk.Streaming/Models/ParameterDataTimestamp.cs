using System;
using System.Collections.Generic;
using Quix.Sdk.Process.Models.Utility;
using Quix.Sdk.Streaming.Utils;

namespace Quix.Sdk.Streaming.Models
{
    /// <summary>
    /// Represents a single point in time with parameter values and tags attached to that time
    /// </summary>
    public readonly struct ParameterDataTimestamp
    {
        internal readonly ParameterData parameterData;
        internal readonly long timestampRawIndex;

        internal ParameterDataTimestamp(ParameterData parameterData, long rawIndex)
        {
            this.parameterData = parameterData;
            this.timestampRawIndex = rawIndex;
        }

        /// <summary>
        /// Parameter values for the timestamp. When a key is not found, returns empty <see cref="ParameterValue"/>
        /// </summary>
        public readonly ParameterDataTimestampValues Parameters => new ParameterDataTimestampValues(this.parameterData, this.timestampRawIndex);

        /// <summary>
        /// Tags for the timestamp. When key is not found, returns null
        /// </summary>
        public readonly ParameterDataTimestampTags Tags  => new ParameterDataTimestampTags(this.parameterData, this.timestampRawIndex);

        /// <summary>
        /// Gets the timestamp in nanoseconds
        /// </summary>
        public long TimestampNanoseconds
        {
            get
            {
                return !this.parameterData.epochsIncluded[this.timestampRawIndex] 
                    ? this.parameterData.rawData.Timestamps[this.timestampRawIndex] + this.parameterData.rawData.Epoch
                    : this.parameterData.rawData.Timestamps[this.timestampRawIndex];
            }
            set
            {
                this.parameterData.rawData.Timestamps[this.timestampRawIndex] = value;
            }
        }

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
        public TimeSpan TimestampAsTimeSpan => this.TimestampNanoseconds.FromNanoseconds();

        internal bool EpochIncluded
        {
            get
            {
                return this.parameterData.epochsIncluded[this.timestampRawIndex];
            }
            set
            {
                this.parameterData.epochsIncluded[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// Adds a new numeric value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Numeric value</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp AddValue(string parameterId, double value)
        {
            if (!this.parameterData.rawData.NumericValues.TryGetValue(parameterId, out var values))
            {
                values = new double?[this.parameterData.rawData.Timestamps.Length];
                this.parameterData.rawData.NumericValues.Add(parameterId, values);

                this.parameterData.parameterList[parameterId] = new Parameter(parameterId, values);
            }

            values[this.timestampRawIndex] = value;

            return this;
        }

        /// <summary>
        /// Adds a new string value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">String value</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp AddValue(string parameterId, string value)
        {
            if (!this.parameterData.rawData.StringValues.TryGetValue(parameterId, out var values))
            {
                values = new string[this.parameterData.rawData.Timestamps.Length];
                this.parameterData.rawData.StringValues.Add(parameterId, values);

                this.parameterData.parameterList[parameterId] = new Parameter(parameterId, values);
            }

            values[this.timestampRawIndex] = value;

            return this;
        }

        /// <summary>
        /// Adds a new binary value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">String value</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp AddValue(string parameterId, byte[] value)
        {
            if (!this.parameterData.rawData.BinaryValues.TryGetValue(parameterId, out var values))
            {
                values = new byte[this.parameterData.rawData.Timestamps.Length][];
                this.parameterData.rawData.BinaryValues.Add(parameterId, values);

                this.parameterData.parameterList[parameterId] = new Parameter(parameterId, values);
            }

            values[this.timestampRawIndex] = value;

            return this;
        }

        /// <summary>
        /// Adds a new value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">The value</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp AddValue(string parameterId, ParameterValue value)
        {
            if (value.Value == null) return this;

            var valueType = value.Type;

            if (valueType == ParameterValueType.Numeric) this.AddValue(parameterId, value.NumericValue ?? 0);
            else if (valueType == ParameterValueType.String) this.AddValue(parameterId, value.StringValue);
            else if (valueType == ParameterValueType.Binary) this.AddValue(parameterId, value.BinaryValue);

            return this;
        }

        /// <summary>
        /// Removes a parameter value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp RemoveValue(string parameterId)
        {
            var parameter = this.Parameters[parameterId];
            var valueType = parameter.Type;

            if (valueType == ParameterValueType.Numeric) parameter.NumericValue = null;
            else if (valueType == ParameterValueType.String) parameter.StringValue = null;
            else if (valueType == ParameterValueType.Binary) parameter.BinaryValue = null;

            return this;
        }

        /// <summary>
        /// Adds a tag to the values
        /// </summary>
        /// <param name="tagId">Tag name</param>
        /// <param name="tagValue">Tag value</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp AddTag(string tagId, string tagValue)
        {
            if (string.IsNullOrWhiteSpace(tagId)) throw new ArgumentNullException(nameof(tagId), "Tag id can't be null or empty");
            if (string.IsNullOrWhiteSpace(tagValue)) throw new ArgumentNullException(nameof(tagValue), $"Tag ({tagId}) value can't be null or empty");

            if (!this.parameterData.rawData.TagValues.TryGetValue(tagId, out var values))
            {
                values = new string[this.parameterData.rawData.Timestamps.Length];
                this.parameterData.rawData.TagValues.Add(tagId, values);
            }

            values[this.timestampRawIndex] = tagValue;

            return this;
        }
        
        /// <summary>
        /// Copies the tags from the specified dictionary.
        /// Conflicting tags will be overwritten
        /// </summary>
        /// <param name="tags">The tags to copy</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp AddTags(IEnumerable<KeyValuePair<string, string>> tags)
        {
            if (tags == null) return this;

            foreach (var tagPair in tags)
            {
                this.AddTag(tagPair.Key, tagPair.Value);
            }

            return this;
        }
        /// <summary>
        /// Removes a tag from the values
        /// </summary>
        /// <param name="tagId">Tag name</param>
        /// <returns>This instance</returns>
        public ParameterDataTimestamp RemoveTag(string tagId)
        {
            this.parameterData.rawData.TagValues.Remove(tagId);

            return this;
        }
    }
}