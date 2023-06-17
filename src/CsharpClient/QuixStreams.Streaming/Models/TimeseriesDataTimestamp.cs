using System;
using System.Collections.Generic;
using QuixStreams.Streaming.Utils;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Represents a single point in time with parameter values and tags attached to that time
    /// </summary>
    public readonly struct TimeseriesDataTimestamp
    {
        internal readonly TimeseriesData TimeseriesData;
        internal readonly long timestampRawIndex;

        internal TimeseriesDataTimestamp(TimeseriesData TimeseriesData, long rawIndex)
        {
            this.TimeseriesData = TimeseriesData;
            this.timestampRawIndex = rawIndex;
        }

        /// <summary>
        /// Parameter values for the timestamp. When a key is not found, returns empty <see cref="ParameterValue"/>
        /// </summary>
        public readonly TimeseriesDataTimestampValues Parameters => new TimeseriesDataTimestampValues(this.TimeseriesData, this.timestampRawIndex);

        /// <summary>
        /// Tags for the timestamp. When key is not found, returns null
        /// </summary>
        public readonly TimeseriesDataTimestampTags Tags  => new TimeseriesDataTimestampTags(this.TimeseriesData, this.timestampRawIndex);

        /// <summary>
        /// Gets the timestamp in nanoseconds
        /// </summary>
        public long TimestampNanoseconds
        {
            get
            {
                return !this.TimeseriesData.epochsIncluded[this.timestampRawIndex]
                    ? this.TimeseriesData.rawData.Timestamps[this.timestampRawIndex] + this.TimeseriesData.rawData.Epoch
                    : this.TimeseriesData.rawData.Timestamps[this.timestampRawIndex];
            }
            set
            {
                this.TimeseriesData.rawData.Timestamps[this.timestampRawIndex] = value;
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
                return this.TimeseriesData.epochsIncluded[this.timestampRawIndex];
            }
            set
            {
                this.TimeseriesData.epochsIncluded[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// Adds a new numeric value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Numeric value</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, double value)
        {
            return AddValue(parameterId, value, overwrite: true);
        }

        /// <summary>
        /// Adds a new numeric value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Numeric value</param>
        /// <param name="overwrite">Indicates whether to overwrite the value for the parameter if it already exists.</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, double value, bool overwrite)
        {
            var valueExists = false;
            if (!this.TimeseriesData.rawData.NumericValues.TryGetValue(parameterId, out var values))
            {
                values = new double?[this.TimeseriesData.rawData.Timestamps.Length];
                this.TimeseriesData.rawData.NumericValues.Add(parameterId, values);

                this.TimeseriesData.parameterList[parameterId] = new Parameter(parameterId, values);
            }
            else
            {
                valueExists = values[this.timestampRawIndex] != null;
            }

            if (overwrite || !valueExists)
            {
                values[this.timestampRawIndex] = value;
            }

            return this;
        }

        /// <summary>
        /// Adds a new string value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">String value</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, string value)
        {
            return AddValue(parameterId, value, overwrite: true);
        }

        /// <summary>
        /// Adds a new string value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">String value</param>
        /// <param name="overwrite">Indicates whether to overwrite the value for the parameter if it already exists.</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, string value, bool overwrite)
        {
            var valueExists = false;
            if (!this.TimeseriesData.rawData.StringValues.TryGetValue(parameterId, out var values))
            {
                values = new string[this.TimeseriesData.rawData.Timestamps.Length];
                this.TimeseriesData.rawData.StringValues.Add(parameterId, values);

                this.TimeseriesData.parameterList[parameterId] = new Parameter(parameterId, values);
            }
            else
            {
                valueExists = values[this.timestampRawIndex] != null;
            }

            if (overwrite || !valueExists)
            {
                values[this.timestampRawIndex] = value;
            }

            return this;
        }

        /// <summary>
        /// Adds a new binary value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Byte array value</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, byte[] value)
        {
            return AddValue(parameterId, value, overwrite: true);
        }

        /// <summary>
        /// Adds a new binary value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Byte array value</param>
        /// <param name="overwrite">Indicates whether to overwrite the value for the parameter if it already exists.</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, byte[] value, bool overwrite)
        {
            var valueExists = false;
            if (!this.TimeseriesData.rawData.BinaryValues.TryGetValue(parameterId, out var values))
            {
                values = new byte[this.TimeseriesData.rawData.Timestamps.Length][];
                this.TimeseriesData.rawData.BinaryValues.Add(parameterId, values);

                this.TimeseriesData.parameterList[parameterId] = new Parameter(parameterId, values);
            }
            else
            {
                valueExists = values[this.timestampRawIndex] != null;
            }

            if (overwrite || !valueExists)
            {
                values[this.timestampRawIndex] = value;
            }

            return this;
        }

        /// <summary>
        /// Adds a new value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">The value</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, ParameterValue value)
        {
            if (value.Value == null) return this;

            var valueType = value.Type;

            if (valueType == ParameterValueType.Numeric) this.AddValue(parameterId, value.NumericValue ?? 0);
            else if (valueType == ParameterValueType.String) this.AddValue(parameterId, value.StringValue);
            else if (valueType == ParameterValueType.Binary) this.AddValue(parameterId, value.BinaryValue);

            return this;
        }

        /// <summary>
        /// Adds a new value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">The value</param>
        /// <param name="overwrite">Indicates whether to overwrite the value for the parameter if it already exists.</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp AddValue(string parameterId, ParameterValue value, bool overwrite)
        {
            if (value.Value == null) return this;

            var valueType = value.Type;

            if (valueType == ParameterValueType.Numeric) this.AddValue(parameterId, value.NumericValue ?? 0, overwrite);
            else if (valueType == ParameterValueType.String) this.AddValue(parameterId, value.StringValue, overwrite);
            else if (valueType == ParameterValueType.Binary) this.AddValue(parameterId, value.BinaryValue, overwrite);

            return this;
        }

        /// <summary>
        /// Removes a parameter value.
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <returns>This instance</returns>
        public TimeseriesDataTimestamp RemoveValue(string parameterId)
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
        public TimeseriesDataTimestamp AddTag(string tagId, string tagValue)
        {
            if (string.IsNullOrWhiteSpace(tagId)) throw new ArgumentNullException(nameof(tagId), "Tag id can't be null or empty");
            if (tagValue == null) return this.RemoveTag(tagId);

            if (!this.TimeseriesData.rawData.TagValues.TryGetValue(tagId, out var values))
            {
                values = new string[this.TimeseriesData.rawData.Timestamps.Length];
                this.TimeseriesData.rawData.TagValues.Add(tagId, values);
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
        public TimeseriesDataTimestamp AddTags(IEnumerable<KeyValuePair<string, string>> tags)
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
        public TimeseriesDataTimestamp RemoveTag(string tagId)
        {
            this.TimeseriesData.rawData.TagValues.Remove(tagId);

            return this;
        }

        internal TimeseriesDataRaw ConvertToTimeseriesDataRaw()
        {
            return new TimeseriesData(new List<TimeseriesDataTimestamp>{this}).rawData;
        }
    }
}