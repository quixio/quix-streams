using System;
using System.Diagnostics;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Represents a single parameter value of either numeric, string or binary type
    /// </summary>
    [DebuggerDisplay("{" + nameof(Value) + "}")]
    public readonly struct ParameterValue
    {
        private readonly long timestampRawIndex;
        private readonly TimeseriesDataParameter timeseriesDataParameter;

        internal ParameterValue(long timestampRawIndex, TimeseriesDataParameter timeseriesDataParameter)
        {
            this.timestampRawIndex = timestampRawIndex;
            this.timeseriesDataParameter = timeseriesDataParameter;
            this.Type = this.timeseriesDataParameter.ValueType;
        }

        /// <summary>
        /// Gets the Parameter Id of the parameter
        /// </summary>
        public readonly string ParameterId => this.timeseriesDataParameter.ParameterId;

        /// <summary>
        /// Gets the type of value, which is numeric, string or binary if set, else empty
        /// </summary>
        public readonly ParameterValueType Type;

        /// <summary>
        /// The numeric value of the parameter.
        /// </summary>
        public double? NumericValue
        {
            get
            {
                return this.timeseriesDataParameter.NumericValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.timeseriesDataParameter.NumericValues == null)
                {
                    throw new InvalidOperationException($"The parameter '{this.ParameterId}' is not of Numeric type.");
                }

                this.timeseriesDataParameter.NumericValues[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// The string value of the parameter
        /// </summary>
        public string StringValue
        {
            get
            {
                return this.timeseriesDataParameter.StringValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.timeseriesDataParameter.StringValues == null)
                {
                    throw new InvalidOperationException($"The parameter '{this.ParameterId}' is not of String type.");
                }

                this.timeseriesDataParameter.StringValues[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// The binary value of the parameter
        /// </summary>
        public byte[] BinaryValue
        {
            get
            {
                return this.timeseriesDataParameter.BinaryValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.timeseriesDataParameter.BinaryValues == null)
                {
                    throw new InvalidOperationException($"The parameter '{this.ParameterId}' is not of Binary type.");
                }

                this.timeseriesDataParameter.BinaryValues[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// Gets the underlying value
        /// </summary>
        public readonly object Value
        {
            get
            {
                return (object)this.timeseriesDataParameter.NumericValues?[this.timestampRawIndex]
                    ?? (object)this.timeseriesDataParameter.StringValues?[this.timestampRawIndex]
                    ?? (object)this.timeseriesDataParameter.BinaryValues?[this.timestampRawIndex];
            }
        }

        /// <summary>
        /// Equality comparison of Parameter values
        /// </summary>
        /// <param name="lhs">First Parameter value to compare</param>
        /// <param name="rhs">Second Parameter value to compare</param>
        /// <returns>Whether the values are equal</returns>
        public static bool operator == (ParameterValue lhs, ParameterValue rhs)
        {
            var lhsValue = lhs.Value;
            var rhsValue = rhs.Value;

            if (lhsValue?.GetType() != rhsValue?.GetType())
            {
                return false;
            }

            if (lhsValue is string)
            {
                return ((string) lhsValue) == ((string) rhsValue);
            }

            if (lhsValue is double?)
            {
                return ((double?) lhsValue) == ((double?) rhsValue);
            }

            if (lhsValue is byte[])
            {
                return ((byte[]) lhsValue) == ((byte[]) rhsValue);
            }

            if (lhsValue == null && rhsValue == null)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Negative equality comparison of Parameter values
        /// </summary>
        /// <param name="lhs">First Parameter value to compare</param>
        /// <param name="rhs">Second Parameter value to compare</param>
        /// <returns>Whether the values are not equal</returns>
        public static bool operator != (ParameterValue lhs, ParameterValue rhs)
        {
            return !(lhs == rhs);
        }

        /// <inheritdoc/>
        public readonly override bool Equals(Object obj)
        {
            return obj is ParameterValue c && this == c;
        }

        /// <inheritdoc/>
        public readonly override int GetHashCode()
        {
            unchecked
            {
                var hash = 397;
                hash ^= this.timestampRawIndex.GetHashCode();
                hash ^= this.timeseriesDataParameter.GetHashCode();

                return hash;
            }
        }
    }
}