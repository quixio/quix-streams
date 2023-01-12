using System;
using System.Diagnostics;

namespace Quix.Sdk.Streaming.Models
{
    /// <summary>
    /// Represents a single parameter value of either numeric, string or binary type
    /// </summary>
    [DebuggerDisplay("{" + nameof(Value) + "}")]
    public readonly struct ParameterValue
    {
        private readonly long timestampRawIndex;
        private readonly Parameter parameter;

        internal ParameterValue(long timestampRawIndex, Parameter parameter)
        {
            this.timestampRawIndex = timestampRawIndex;
            this.parameter = parameter;
        }

        /// <summary>
        /// Gets the Parameter Id of the parameter
        /// </summary>
        public readonly string ParameterId => this.parameter.ParameterId;

        /// <summary>
        /// Gets the type of value, which is numeric, string or binary if set, else empty
        /// </summary>
        public readonly ParameterValueType Type
        {
            get
            {
                if (this.parameter.NumericValues != null) return ParameterValueType.Numeric;
                else if (this.parameter.StringValues != null) return ParameterValueType.String;
                else if (this.parameter.BinaryValues != null) return ParameterValueType.Binary;
                else return ParameterValueType.Empty;
            }
        }

        /// <summary>
        /// The numeric value of the parameter.
        /// </summary>
        public double? NumericValue
        {
            get
            {
                return this.parameter.NumericValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.parameter.NumericValues == null)
                {
                    throw new InvalidOperationException($"The parameter '{this.ParameterId}' is not of Numeric type.");
                }

                this.parameter.NumericValues[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// The string value of the parameter
        /// </summary>
        public string StringValue
        {
            get
            {
                return this.parameter.StringValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.parameter.StringValues == null)
                {
                    throw new InvalidOperationException($"The parameter '{this.ParameterId}' is not of String type.");
                }

                this.parameter.StringValues[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// The binary value of the parameter
        /// </summary>
        public byte[] BinaryValue
        {
            get
            {
                return this.parameter.BinaryValues?[this.timestampRawIndex];
            }
            set
            {
                if (this.parameter.BinaryValues == null)
                {
                    throw new InvalidOperationException($"The parameter '{this.ParameterId}' is not of Binary type.");
                }

                this.parameter.BinaryValues[this.timestampRawIndex] = value;
            }
        }

        /// <summary>
        /// Gets the underlying value
        /// </summary>
        public readonly object Value
        {
            get
            {
                return (object)this.parameter.NumericValues?[this.timestampRawIndex]
                    ?? (object)this.parameter.StringValues?[this.timestampRawIndex]
                    ?? (object)this.parameter.BinaryValues?[this.timestampRawIndex];
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
                hash ^= this.parameter.GetHashCode();

                return hash;
            }
        }
    }
}