using System;

namespace QuixStreams.State
{
    /// <summary>
    /// A wrapper class for values that can be stored inside the storage.
    /// </summary>
    public class StateValue : IEquatable<StateValue>
    {
        private readonly string stringValue;
        private readonly byte[] byteValue;
        private readonly bool boolValue;
        private readonly long longValue;
        private readonly double doubleValue;

        /// <summary>
        /// Type of State value
        /// </summary>
        public enum StateType
        {
            /// <summary>
            /// Binary type
            /// </summary>
            Binary = 0,

            /// <summary>
            /// Long type
            /// </summary>
            Long = 2,

            /// <summary>
            /// Boolean type
            /// </summary>
            Bool = 1,

            /// <summary>
            /// Double type
            /// </summary>
            Double = 3,

            /// <summary>
            /// String type
            /// </summary>
            String = 4,
            
            /// <summary>
            /// Object type. It is stored as binary with type of object for easier identification for special handling
            /// </summary>
            Object = 5
        }

        /// <summary>
        /// Get the type of the State value
        /// </summary>
        public StateType Type { get; protected set; }

        /// <summary>
        /// Initializes a new instance of <see cref="StateValue"/>
        /// </summary>
        /// <param name="value">Boolean value</param>
        public StateValue(bool value)
        {
            this.boolValue = value;
            this.Type = StateType.Bool;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StateValue"/>
        /// </summary>
        /// <param name="value">Long value</param>
        public StateValue(long value)
        {
            this.longValue = value;
            this.Type = StateType.Long;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StateValue"/>
        /// </summary>
        /// <param name="value">Binary value as byte array</param>
        public StateValue(byte[] value)
        {
            this.byteValue = value;
            this.Type = StateType.Binary;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StateValue"/>
        /// </summary>
        /// <param name="value">Binary value as byte array</param>
        /// <param name="type">State value type to assign. Only Binary or Object type allowed.</param>
        public StateValue(byte[] value, StateType type)
        {
            if (type != StateType.Binary && type != StateType.Object) throw new ArgumentOutOfRangeException("Byte[] value type must be either binary or object");
            this.byteValue = value;
            this.Type = type;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StateValue"/>
        /// </summary>
        /// <param name="value">String value</param>
        public StateValue(string value)
        {
            this.stringValue = value;
            this.Type = StateType.String;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StateValue"/>
        /// </summary>
        /// <param name="value">Double value</param>
        public StateValue(double value)
        {
            this.doubleValue = value;
            this.Type = StateType.Double;
        }

        /// <summary>
        /// Get the value as Double
        /// </summary>
        public double DoubleValue
        {
            get
            {
                if (Type != StateType.Double)
                {
                    throw new InvalidCastException("value is not double type");
                }
                return this.doubleValue;
            }
        }

        /// <summary>
        /// Get the value as Long
        /// </summary>
        public long LongValue
        {
            get
            {
                if (Type != StateType.Long)
                {
                    throw new InvalidCastException("value is not long type");
                }
                return this.longValue;
            }
        }

        /// <summary>
        /// Get the value as String
        /// </summary>
        public string StringValue {
            get {
                if (Type != StateType.String)
                {
                    throw new InvalidCastException("value is not string type");
                }
                return this.stringValue;
            }
        }

        /// <summary>
        /// Get the value as Boolean
        /// </summary>
        public bool BoolValue
        {
            get
            {
                if (Type != StateType.Bool)
                {
                    throw new InvalidCastException("value is not boolean type");
                }
                return this.boolValue;
            }
        }

        /// <summary>
        /// Get the value as Binary
        /// </summary>
        public byte[] BinaryValue
        {
            get
            {
                if (Type != StateType.Binary && Type != StateType.Object)
                {
                    throw new InvalidCastException("value is not binary type");
                }
                return this.byteValue;
            }
        }

        /// <summary>
        /// Returns whether the State value is null or not if the allows
        /// </summary>
        /// <returns>True if null, false otherwise</returns>
        public bool IsNull()
        {
            switch (this.Type)
            {
                case StateType.Binary:
                    return this.BinaryValue == null;
                case StateType.Long:
                case StateType.Bool:
                case StateType.Double:
                    return false;
                case StateType.String:
                    return this.StringValue == null;
                case StateType.Object:
                    return this.BinaryValue == null;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            switch (this.Type)
            {
                case StateType.Binary:
                    return this.BinaryValue?.GetHashCode() ?? 0;
                case StateType.Long:
                    return this.LongValue.GetHashCode();
                case StateType.Bool:
                    return this.BoolValue.GetHashCode();
                case StateType.Double:
                    return this.DoubleValue.GetHashCode();
                case StateType.String:
                    return this.StringValue?.GetHashCode() ?? 0;
                case StateType.Object:
                    return this.BinaryValue?.GetHashCode() ?? 0;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// Equals object to the another of kind
        /// </summary>
        /// <param name="other">Value to compare to</param>
        /// <returns>Whether the Values are equal or not</returns>
        public bool Equals(StateValue other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (this.Type != other.Type) return false;
            switch (this.Type)
            {
                case StateType.Binary:
                    var b1 = this.BinaryValue;
                    var b2 = other.BinaryValue;
                    if (b1.Length != b2.Length)
                        return false;
                    for(var i = 0; i < b1.Length; ++i)
                    {
                        if (b1[i] != b2[i])
                            return false;
                    }
                    return true;
                case StateType.Bool:
                    return this.BoolValue == other.BoolValue;
                case StateType.Double:
                    return Math.Abs(this.DoubleValue - other.DoubleValue) < Double.Epsilon;
                case StateType.Long:
                    return this.LongValue == other.LongValue;
                case StateType.String:
                    return this.StringValue == other.StringValue;
                case StateType.Object:
                    return this.BinaryValue == other.BinaryValue;                
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
