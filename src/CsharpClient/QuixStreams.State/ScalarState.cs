using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using QuixStreams.State.Storage;

namespace QuixStreams.State
{
    /// <summary>
    /// Represents a state container that stores scalar value with the ability to flush changes to a specified storage.
    /// </summary>
    public class ScalarState: IState
    {
        /// <summary>
        /// Represents the storage where the state changes will be persisted.
        /// </summary>
        private readonly IStateStorage storage;
        
        /// <summary>
        /// Indicates whether the storage should be cleared before flushing the changes.
        /// </summary>
        private bool clearBeforeFlush;

        /// <summary>
        /// Represents the in-memory state holding the state value.
        /// </summary>
        private StateValue inMemoryValue = null;
        
        /// <summary>
        /// Represents the key used to store the state in the storage.
        /// </summary>
        public const string StorageKey = "SCALAR";

        /// <summary>
        /// Represents the hash of last flushed value that is persisted
        /// </summary>
        private int lastFlushHash = default;
        
        /// <summary>
        /// The logger for the class
        /// </summary>
        private readonly ILogger<DictionaryState> logger;
        
        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryState"/> class using the specified storage.
        /// </summary>
        /// <param name="storage">An instance of <see cref="IStateStorage"/> that represents the storage to persist state changes to.</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        /// <exception cref="ArgumentNullException">Thrown when the storage parameter is null.</exception>
        public ScalarState(IStateStorage storage, ILoggerFactory loggerFactory = null)
        {
            this.logger = loggerFactory?.CreateLogger<DictionaryState>() ?? NullLogger<DictionaryState>.Instance;
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            inMemoryValue = this.storage.ContainsKey(StorageKey) ? this.storage.Get(StorageKey) : null;
        }

        /// <summary>
        /// Sets the value of in-memory state to null and marks the state for clearing when flushed.
        /// </summary>
        public void Clear()
        {
            inMemoryValue = null;
            clearBeforeFlush = true;
        }

        /// <summary>
        /// Gets or sets the value to the in-memory state.
        /// </summary>
        /// <returns>Returns the value</returns>
        public StateValue Value
        {
            get => inMemoryValue;
            set => inMemoryValue = value;
        }
        
        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            this.logger.LogTrace("Flushing state.");
            OnFlushing?.Invoke(this, EventArgs.Empty);
            
            if (this.clearBeforeFlush)
            {
                this.storage.Clear();
                this.clearBeforeFlush = false;
            }
            
            if (Value == null || Value.IsNull())
            {
                this.lastFlushHash = default;
                storage.Remove(StorageKey);
            }
            else
            {
                var hash = inMemoryValue.GetHashCode();
                if (lastFlushHash != hash)
                {
                    this.lastFlushHash = hash;
                    this.storage.Set(StorageKey, inMemoryValue);
                }
            }

            if (storage.CanPerformTransactions)
            {
                storage.Flush();
            }
            
            OnFlushed?.Invoke(this, EventArgs.Empty);
            this.logger.LogTrace("Flushed.");
        }

        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            if (inMemoryValue.GetHashCode() == lastFlushHash)
            {
                this.logger.LogTrace("Resetting state not needed, hash is the same.");
                return;
            }
            this.logger.LogTrace("Resetting state");
            
            // Retrieve values from storage
            this.inMemoryValue = this.storage.Get(StorageKey);
            
            this.logger.LogTrace($"Reset state completed.");
        }
        
        /// <summary>
        /// Releases storage resources used by the state.
        /// </summary>
        public void Dispose()
        {
            this.storage.Dispose();
        }
    }
    
    /// <summary>
    /// Represents a state container that stores scalar value with the ability to flush changes to a specified storage.
    /// </summary>
    public class ScalarState<T> : IState
    {
        /// <summary>
        /// The logger for the class
        /// </summary>
        private readonly ILogger logger;
        

        /// <summary>
        /// Indicates whether the state should be cleared before flushing the changes.
        /// </summary>
        private bool clearBeforeFlush;
        
        /// <summary>
        /// The underlying state storage for this State, responsible for managing the actual value.
        /// </summary>
        private readonly ScalarState underlyingScalarState;

        /// <summary>
        /// A function that converts a StateValue to the desired value of type T, using appropriate conversion logic based on the type of T.
        /// </summary>
        private readonly Func<StateValue, T> genericConverter;
        
        /// <summary>
        /// A function that converts a value of type T to a StateValue, using appropriate conversion logic based on the type of T.
        /// </summary>
        private readonly Func<T, StateValue> stateValueConverter;

        /// <summary>
        /// The in-memory cache to avoid conversions between State and T whenever possible
        /// </summary>
        private StateValue inMemoryValue = default;

        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryState"/> class with the specified storage and logger factory.
        /// </summary>
        /// <param name="storage">The storage provider to persist state changes to. Must not be null.</param>
        /// <param name="loggerFactory">Optional logger factory to enable logging from within the state object.</param>
        /// <exception cref="ArgumentNullException">Thrown when the storage parameter is null.</exception>
        public ScalarState(IStateStorage storage, ILoggerFactory loggerFactory = null) : this(new ScalarState(storage, loggerFactory), loggerFactory)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryState"/> class with the specified storage and logger factory.
        /// </summary>
        /// <param name="scalarState">The state to persist state changes to. Must not be null.</param>
        /// <param name="loggerFactory">Optional logger factory to enable logging from within the state object.</param>
        /// <exception cref="ArgumentNullException">Thrown when the storage parameter is null.</exception>
        public ScalarState(ScalarState scalarState, ILoggerFactory loggerFactory = null)
        {
            this.underlyingScalarState = scalarState ?? throw new ArgumentNullException(nameof(scalarState));
            this.logger = loggerFactory?.CreateLogger<DictionaryState<T>>() ?? new NullLogger<DictionaryState<T>>();
            var type = typeof(T);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    throw new ArgumentException(
                        $"{Type.GetTypeCode(type)} is not supported by {nameof(DictionaryState<T>)}.");
                case TypeCode.Object:
                    var options = new JsonSerializerSettings()
                    {
                        Formatting = Formatting.None
                    };
                    genericConverter = value => value.Type == StateValue.StateType.Object 
                        ? default
                        : JsonConvert.DeserializeObject<T>(value.StringValue);
                    stateValueConverter = value => value == null 
                        ? new StateValue(null, StateValue.StateType.Object) 
                        : new StateValue(JsonConvert.SerializeObject(value, options));
                    break;
                case TypeCode.Boolean:
                    genericConverter = value => (T)(object)value.BoolValue;
                    stateValueConverter = value => new StateValue((bool)(object)value);
                    break;
                case TypeCode.Char:
                    genericConverter = value => (T)(object)(char)value.LongValue;
                    stateValueConverter = value => new StateValue((char)(object)value);
                    break;
                case TypeCode.SByte:
                    genericConverter = value => (T)(object)(sbyte)value.LongValue;
                    stateValueConverter = value => new StateValue((sbyte)(object)value);
                    break;
                case TypeCode.Byte:
                    genericConverter = value => (T)(object)(byte)value.LongValue;
                    stateValueConverter = value => new StateValue((byte)(object)value);
                    break;
                case TypeCode.Int16:
                    genericConverter = value => (T)(object)(short)value.LongValue;
                    stateValueConverter = value => new StateValue((short)(object)value);
                    break;
                case TypeCode.UInt16:
                    genericConverter = value => (T)(object)(ushort)value.LongValue;
                    stateValueConverter = value => new StateValue((ushort)(object)value);
                    break;
                case TypeCode.Int32:
                    genericConverter = value => (T)(object)(int)value.LongValue;
                    stateValueConverter = value => new StateValue((int)(object)value);
                    break;
                case TypeCode.UInt32:
                    genericConverter = value => (T)(object)(uint)value.LongValue;
                    stateValueConverter = value => new StateValue((uint)(object)value);
                    break;
                case TypeCode.Int64:
                    genericConverter = value => (T)(object)value.LongValue;
                    stateValueConverter = value => new StateValue((long)(object)value);
                    break;
                case TypeCode.UInt64:
                    genericConverter = value => (T)(object)BitConverter.ToUInt64(value.BinaryValue, 0);
                    stateValueConverter = value =>
                        new StateValue(BitConverter.GetBytes((ulong)(object)value));
                    break;
                case TypeCode.Single:
                    genericConverter = value => (T)(object)(float)value.DoubleValue;
                    stateValueConverter = value => new StateValue((float)(object)value);
                    break;
                case TypeCode.Double:
                    genericConverter = value => (T)(object)value.DoubleValue;
                    stateValueConverter = value => new StateValue((double)(object)value);
                    break;
                case TypeCode.Decimal:
                    genericConverter = value =>
                    {
                        var bytes = value.BinaryValue;
                        var bits = new int[4];
                        for (var ii = 0; ii < 4; ii++)
                        {
                            bits[ii] = BitConverter.ToInt32(bytes, ii * 4);
                        }

                        return (T)(object)new decimal(bits);
                    };
                    stateValueConverter = value =>
                    {
                        var bits = decimal.GetBits((decimal)(object)value);
                        var binaryBytes = new byte[16];
                        for (var ii = 0; ii < 4; ii++)
                        {
                            var intBytes = BitConverter.GetBytes(bits[ii]);
                            Array.Copy(intBytes, 0, binaryBytes, ii * 4, 4);
                        }

                        return new StateValue(binaryBytes);
                    };
                    break;
                case TypeCode.DateTime:
                    genericConverter = value => (T)(object)DateTime.FromBinary(value.LongValue);
                    stateValueConverter = value => new StateValue(((DateTime)(object)value).ToBinary());
                    break;
                case TypeCode.String:
                    genericConverter = value => (T)(object)value.StringValue;
                    stateValueConverter = value => new StateValue((string)(object)value);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            this.inMemoryValue = underlyingScalarState.Value;
        }
        
        /// <summary>
        /// Sets the value of in-memory state to null and marks the state for clearing when flushed.
        /// </summary>
        public void Clear()
        {
            this.inMemoryValue = default;
            this.clearBeforeFlush = true;
        }
        
        /// <summary>
        /// Gets or sets the value to the in-memory state.
        /// </summary>
        /// <returns>Returns the value</returns>
        public T Value
        {
            get => this.inMemoryValue == null
                ? default 
                : genericConverter(this.inMemoryValue);
            set => this.inMemoryValue = stateValueConverter(value);
        }
        
        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            this.logger.LogTrace("Flushing state");
            OnFlushing?.Invoke(this, EventArgs.Empty);
            
            if (this.clearBeforeFlush)
            {
                logger.LogTrace("Clearing state before flush as clear was requested");
                this.underlyingScalarState.Clear();
                this.clearBeforeFlush = false;
            }
            
            logger.LogTrace("Updating value from state as part of flush");
            this.underlyingScalarState.Value = inMemoryValue;

            logger.LogTrace("Flushing underlying state as part of flush");
            this.underlyingScalarState.Flush();
            logger.LogTrace("Flushed underlying state as part of flush");
            OnFlushed?.Invoke(this, EventArgs.Empty);
            this.logger.LogTrace("Flushed state.");
        }
        
        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            this.logger.LogTrace("Resetting state");
            
            this.inMemoryValue = this.underlyingScalarState.Value;
            
            this.logger.LogTrace($"Reset state completed.");
        }
        
        /// <summary>
        /// Releases storage resources used by the state.
        /// </summary>
        public void Dispose()
        {
            this.underlyingScalarState.Dispose();
        }
    }
}