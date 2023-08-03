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

        /// <summary>
        /// Initializes a new instance with the of <see cref="ParameterValue"/> with the specified index and parameter
        /// </summary>
        /// <param name="timestampRawIndex">The index to reference to in the parameter</param>
        /// <param name="timeseriesDataParameter">The parameter the value will be derived from</param>
        public ParameterValue(long timestampRawIndex, TimeseriesDataParameter timeseriesDataParameter)
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
                if (this.Type != ParameterValueType.Numeric) return null;
                return this.timeseriesDataParameter.NumericValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.Type != ParameterValueType.Numeric)
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
                if (this.Type != ParameterValueType.String) return null;
                return this.timeseriesDataParameter.StringValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.Type != ParameterValueType.String)
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
                if (this.Type != ParameterValueType.Binary) return null;
                return this.timeseriesDataParameter.BinaryValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.Type != ParameterValueType.Binary)
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
                switch (Type)
                {
                    case ParameterValueType.Empty:
                        return null;
                    case ParameterValueType.Numeric:
                        return this.timeseriesDataParameter.NumericValues?[this.timestampRawIndex];
                    case ParameterValueType.String:
                        return this.timeseriesDataParameter.StringValues?[this.timestampRawIndex];
                    case ParameterValueType.Binary:
                        return this.timeseriesDataParameter.BinaryValues?[this.timestampRawIndex];
                    default:
                        throw new ArgumentOutOfRangeException();
                }
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

            if (lhs.Type != rhs.Type)
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