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
    /// Represents a state container that stores key-value pairs with the ability to flush changes to a specified storage.
    /// </summary>
    public class DictionaryState : IState, IDictionary<string, StateValue>, IDictionary
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
        /// Represents the in-memory state holding the key-value pairs.
        /// </summary>
        private readonly Dictionary<string, StateValue> inMemoryState = new Dictionary<string, StateValue>();
        
        /// <summary>
        /// Represents the in-memory state holding the key-value pairs of last persisted hash values
        /// </summary>
        private readonly IDictionary<string, int> lastFlushHash = new Dictionary<string, int>();
        
        /// <summary>
        /// Represents the changes made to the in-memory state, tracking additions, updates, and removals.
        /// </summary>
        private readonly IDictionary<string, ChangeType> changes = new Dictionary<string, ChangeType>();

        /// <summary>
        /// The logger for the class
        /// </summary>
        private readonly ILogger<DictionaryState> logger;

        /// <summary>
        /// Returns whether the cache keys are case-sensitive
        /// </summary>
        public bool IsCaseSensitive => this.storage.IsCaseSensitive;
        
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
        public DictionaryState(IStateStorage storage, ILoggerFactory loggerFactory = null)
        {
            this.logger = loggerFactory?.CreateLogger<DictionaryState>() ?? NullLogger<DictionaryState>.Instance;
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            var keys = this.storage.GetAllKeys();
            foreach (var key in keys)
            {
                inMemoryState[key] = this.storage.Get(key);
            }
        }

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        public bool Contains(object key)
        {
            return this.ContainsKey((string)key);
        }

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            lock (this.SyncRoot)
            {
                return this.inMemoryState.ToDictionary(y=> y.Key, y=> y.Value).GetEnumerator();
            }
        }

        /// <inheritdoc />
        public void Remove(object key)
        {
            this.Remove((string)key);
        }

        /// <inheritdoc />
        public bool IsFixedSize => false;

        /// <summary>
        /// Returns an enumerator that iterates through the in-memory state.
        /// </summary>
        /// <returns>An enumerator for the in-memory state.</returns>
        public IEnumerator<KeyValuePair<string, StateValue>> GetEnumerator()
        {
            lock (this.SyncRoot)
            {
                return inMemoryState.ToDictionary(y => y.Key, y => y.Value).GetEnumerator();
            }
        }

        /// <inheritdoc />
        public void Add(object key, object value)
        {
            this.Add((string)key, (StateValue)value);
        }

        /// <inheritdoc cref="IState"/>
        public void Clear()
        {
            lock (this.SyncRoot)
            {
                inMemoryState.Clear();
                changes.Clear();
                clearBeforeFlush = true;
            }
        }
        
        /// <inheritdoc/>
        public void Add(KeyValuePair<string, StateValue> item)
        {
            this.Add(item.Key, item.Value);
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, StateValue> item)
        {
            lock (this.SyncRoot)
            {
                return this.inMemoryState.TryGetValue(item.Key, out var existing) && existing.Equals(item.Value);
            }
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, StateValue>[] array, int arrayIndex)
        {
            lock (this.SyncRoot)
            {
                ((IDictionary<string, StateValue>)this.inMemoryState).CopyTo(array, arrayIndex);
            }
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, StateValue> item)
        {
            lock (this.SyncRoot)
            {
                return this.Contains(item) && this.Remove(item.Key);
            }
        }

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        public bool IsReadOnly => false;

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        public object this[object key]
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this[(string)key];
                }
            }
            set
            {
                lock (this.SyncRoot)
                {
                    this[(string)key] = (StateValue)value;
                }
            }
        }

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        public void CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the number of key-value pairs contained in the in-memory state.
        /// </summary>
        public int Count => inMemoryState.Count;

        /// <inheritdoc cref="IDictionary.IsSynchronized" />
        public bool IsSynchronized => ((IDictionary)this.inMemoryState).IsSynchronized;
        
        /// <inheritdoc cref="IDictionary.SyncRoot" />
        public object SyncRoot => ((IDictionary)this.inMemoryState).SyncRoot;

        /// <summary>
        /// Adds the specified key and value to the in-memory state and marks the entry for addition or update when flushed.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">The value of the element to add.</param>
        public void Add(string key, StateValue value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                inMemoryState.Add(key, value);
                this.changes[key] = value == null || value.IsNull()
                    ? ChangeType.Removed
                    : ChangeType.AddedOrUpdated;
            }
        }

        /// <summary>
        /// Determines whether the in-memory state contains an element with the specified key.
        /// </summary>
        /// <param name="key">The key to locate in the in-memory state.</param>
        /// <returns>true if the in-memory state contains an element with the key; otherwise, false.</returns>
        public bool ContainsKey(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                return inMemoryState.ContainsKey(key);
            }
        }

        /// <summary>
        /// Removes the element with the specified key from the in-memory state and marks the entry for removal when flushed.
        /// </summary>
        /// <param name="key">The key of the element to remove.</param>
        /// <returns>true if the element is successfully removed; otherwise, false.</returns>
        public bool Remove(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                var success = inMemoryState.Remove(key);
                if (success) this.changes[key] = ChangeType.Removed;
                return success;
            }
        }

        /// <summary>
        /// Gets the value associated with the specified key from the in-memory state.
        /// </summary>
        /// <param name="key">The key of the value to get.</param>
        /// <param name="value">When this method returns, the value associated with the specified key, if the key is found; otherwise, the default value for the type of the value parameter.</param>
        /// <returns>true if the in-memory state contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue(string key, out StateValue value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                return inMemoryState.TryGetValue(key, out value);
            }
        }

        /// <summary>
        /// Gets or sets the element with the specified key in the in-memory state.
        /// </summary>
        /// <param name="key">The key of the element to get or set.</param>
        /// <returns>The element with the specified key.</returns>
        public StateValue this[string key]
        {
            get
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                lock (this.SyncRoot)
                {
                    return this.inMemoryState[key];
                }
            }
            set
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                lock (this.SyncRoot)
                {
                    if (value == null || value.IsNull()) this.changes[key] = ChangeType.Removed;
                    else this.changes[key] = ChangeType.AddedOrUpdated;
                    this.inMemoryState[key] = value;
                }
            }
        }

        /// <summary>
        /// Gets an ICollection containing the keys of the in-memory state.
        /// </summary>
        public ICollection<string> Keys
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this.inMemoryState.Keys.ToArray();
                }
            }
        }

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        ICollection IDictionary.Values
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this.inMemoryState.Values.ToArray();
                }
            }
        }

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        ICollection IDictionary.Keys
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this.inMemoryState.Keys.ToArray();
                }
            }
        }

        /// <summary>
        /// Gets an ICollection containing the values of the in-memory state.
        /// </summary>
        public ICollection<StateValue> Values
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this.inMemoryState.Values.ToArray();
                }
            }
        }

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            lock (this.SyncRoot)
            {
                this.logger.LogTrace("Flushing state.");
                OnFlushing?.Invoke(this, EventArgs.Empty);

                if (this.clearBeforeFlush)
                {
                    this.storage.Clear();
                    this.clearBeforeFlush = false;
                }

                var tasks = new List<Task>();
                foreach (var changeType in changes)
                {
                    if (changeType.Value == ChangeType.Removed)
                    {
                        this.lastFlushHash.Remove(changeType.Key);
                        tasks.Add(this.storage.RemoveAsync(changeType.Key));
                    }
                    else
                    {
                        var value = inMemoryState[changeType.Key];
                        if (value == null || value.IsNull())
                        {
                            this.lastFlushHash.Remove(changeType.Key);
                            tasks.Add(this.storage.RemoveAsync(changeType.Key));
                        }
                        else
                        {
                            var hash = value.GetHashCode();
                            if (lastFlushHash.TryGetValue(changeType.Key, out var existingHash) && existingHash == hash)
                                continue;
                            this.lastFlushHash[changeType.Key] = hash;
                            tasks.Add(this.storage.SetAsync(changeType.Key, inMemoryState[changeType.Key]));
                        }
                    }
                }

                this.changes.Clear();
                Task.WaitAll(tasks.ToArray());

                if (this.storage.CanPerformTransactions)
                {
                    this.storage.Flush();
                }

                OnFlushed?.Invoke(this, EventArgs.Empty);
                this.logger.LogTrace("Flushed {0} state changes.", tasks.Count());
            }
        }

        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            lock (this.SyncRoot)
            {
                if (this.changes.Count == 0)
                {
                    this.logger.LogTrace("Resetting state not needed, empty");
                    return;
                }

                this.logger.LogTrace("Resetting state");
                // Remove current values
                foreach (var changeType in this.changes)
                {
                    this.inMemoryState.Remove(changeType.Key);
                }

                // Retrieve values from storage
                var tasks = this.changes.Select(async y =>
                {
                    try
                    {
                        var value = await this.storage.GetAsync(y.Key);
                        return (y.Key, value);
                    }
                    catch
                    {
                        // was added
                        return (y.Key, null);
                    }
                }).ToArray();

                Task.WaitAll(tasks);

                // Assign result to in-memory
                foreach (var task in tasks)
                {
                    var (key, value) = task.Result;
                    if (value == null) continue;
                    this.inMemoryState[key] = value;
                }

                // Reset changes
                var count = this.changes.Count;
                this.changes.Clear();
                this.logger.LogTrace($"Reset {count} state");
            }
        }
        
        /// <summary>
        /// Releases storage resources used by the state.
        /// </summary>
        public void Dispose()
        {
            this.storage.Dispose();
        }

        /// <summary>
        /// Represents the types of changes made to the state.
        /// </summary>
        private enum ChangeType
        {
            Removed,
            AddedOrUpdated
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
    
    /// <summary>
    /// Represents a state container that stores key-value pairs with the ability to flush changes to a specified storage.
    /// </summary>
    public class DictionaryState<T> : IState, IDictionary<string, T>
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
        /// The underlying state storage for this State, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly DictionaryState underlyingDictionaryState;

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
        private readonly IDictionary<string, StateValue> inMemoryCache = new Dictionary<string, StateValue>();
        
        /// <summary>
        /// Represents the changes made to the in-memory state, tracking additions, updates, and removals.
        /// </summary>
        private readonly IDictionary<string, ChangeType> changes = new Dictionary<string, ChangeType>();
        
        /// <summary>
        /// Returns whether the cache keys are case-sensitive
        /// </summary>
        public bool IsCaseSensitive => this.underlyingDictionaryState.IsCaseSensitive;

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
        public DictionaryState(IStateStorage storage, ILoggerFactory loggerFactory = null) : this(new DictionaryState(storage, loggerFactory), loggerFactory)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryState"/> class with the specified storage and logger factory.
        /// </summary>
        /// <param name="dictionaryState">The state to persist state changes to. Must not be null.</param>
        /// <param name="loggerFactory">Optional logger factory to enable logging from within the state object.</param>
        /// <exception cref="ArgumentNullException">Thrown when the storage parameter is null.</exception>
        public DictionaryState(DictionaryState dictionaryState, ILoggerFactory loggerFactory = null)
        {
            this.underlyingDictionaryState = dictionaryState ?? throw new ArgumentNullException(nameof(dictionaryState));
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

            foreach (var pair in this.underlyingDictionaryState)
            {
                this.inMemoryCache[pair.Key] = pair.Value;
            }
        }
        
        /// <inheritdoc cref="IDictionary.SyncRoot" />
        public object SyncRoot => ((IDictionary)this.inMemoryCache).SyncRoot;

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        {
            lock (this.SyncRoot)
            {
                var enumerable = this.inMemoryCache.ToDictionary(y=> y.Key, y=> genericConverter(y.Value));
                return enumerable.GetEnumerator();
            }
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc/>
        public void Add(KeyValuePair<string, T> item)
        {
            this.Add(item.Key, item.Value);
        }

        /// <inheritdoc/>
        public void Clear()
        {
            lock (this.SyncRoot)
            {
                this.inMemoryCache.Clear();
                this.clearBeforeFlush = true;
                this.changes.Clear();
            }
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, T> item)
        {
            lock (this.SyncRoot)
            {
                return this.inMemoryCache.TryGetValue(item.Key, out var existing) && existing.Equals(item.Value);
            }
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, T> item)
        {
            lock (this.SyncRoot)
            {
                return this.Contains(item) && this.Remove(item.Key);
            }
        }

        /// <inheritdoc/>
        public int Count => this.inMemoryCache.Count;
        
        /// <inheritdoc/>
        public bool IsReadOnly => false;
        
        /// <inheritdoc/>
        public void Add(string key, T value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                inMemoryCache.Add(key, stateValueConverter(value));
                this.changes[key] = value == null
                    ? ChangeType.Removed
                    : ChangeType.AddedOrUpdated;
            }
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                return this.inMemoryCache.ContainsKey(key);
            }
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                this.changes[key] = ChangeType.Removed;
                return this.inMemoryCache.Remove(key);
            }
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            lock (this.SyncRoot)
            {
                if (!inMemoryCache.TryGetValue(key, out var stateValue))
                {
                    value = default;
                    return false;
                }

                value = genericConverter(stateValue);
                return true;
            }
        }

        /// <inheritdoc/>
        public T this[string key]
        {
            get
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                lock (this.SyncRoot)
                {
                    if (this.TryGetValue(key, out T val)) return val;
                    this.inMemoryCache[key] = stateValueConverter(val);
                    this.changes[key] = ChangeType.AddedOrUpdated;
                    return val;
                }
            }
            set
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                lock (this.SyncRoot)
                {
                    this.changes[key] = value == null
                        ? ChangeType.Removed
                        : ChangeType.AddedOrUpdated;
                    this.inMemoryCache[key] = stateValueConverter(value);
                }
            }
        }

        /// <inheritdoc/>
        public ICollection<string> Keys
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this.inMemoryCache.Keys.ToArray();
                }
            }
        }

        /// <inheritdoc/>
        public ICollection<T> Values
        {
            get
            {
                lock (this.SyncRoot)
                {
                    return this.inMemoryCache.Values.Select(y => genericConverter(y)).ToArray();
                }
            }
        }

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            lock (this.SyncRoot)
            {
                this.logger.LogTrace("Flushing state");
                OnFlushing?.Invoke(this, EventArgs.Empty);

                if (this.clearBeforeFlush)
                {
                    logger.LogTrace("Clearing state before flush as clear was requested");
                    this.underlyingDictionaryState.Clear();
                    this.clearBeforeFlush = false;
                }

                // If the instance is not type by reference, loop through each change in the list of changes
                foreach (var changeType in changes)
                {
                    // For any change that has a value of "Removed", remove the corresponding key from the internal state
                    if (changeType.Value == ChangeType.Removed)
                    {
                        this.underlyingDictionaryState.Remove(changeType.Key);
                        logger.LogTrace("Removing key '{0}' from state as part of flush", changeType.Key);
                    }
                    else
                    {
                        // For any change that is not "Removed", look up the corresponding value in the in-memory cache
                        // Apply the state value converter to the value and store the result in the internal state with the same key
                        this.underlyingDictionaryState[changeType.Key] = inMemoryCache[changeType.Key];
                        logger.LogTrace("Updating key '{0}' from state as part of flush", changeType.Key);
                    }
                }

                this.changes.Clear();

                logger.LogTrace("Flushing underlying state as part of flush");
                this.underlyingDictionaryState.Flush();
                logger.LogTrace("Flushed underlying state as part of flush");
                OnFlushed?.Invoke(this, EventArgs.Empty);
                this.logger.LogTrace("Flushed state.");
            }
        }
        
        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            lock (this.SyncRoot)
            {
                if (this.changes.Count == 0)
                {
                    this.logger.LogTrace("Resetting state not needed, empty");
                    return;
                }

                this.logger.LogTrace("Resetting state");

                // Retrieve values from storage
                foreach (var change in this.changes)
                {
                    try
                    {
                        this.inMemoryCache.Remove(change.Key); // Remove current value
                        var value = this.underlyingDictionaryState[change.Key];
                        if (value == null) continue;
                        this.inMemoryCache[change.Key] = value; // set original value
                    }
                    catch
                    {
                        // was added, missing key
                    }
                }

                // Reset changes
                var count = this.changes.Count;
                this.changes.Clear();
                this.logger.LogTrace($"Reset {count} state");
            }
        }
        
        /// <summary>
        /// Releases storage resources used by the state.
        /// </summary>
        public void Dispose()
        {
            this.underlyingDictionaryState.Dispose();
        }
        
        /// <summary>
        /// Represents the types of changes made to the state.
        /// </summary>
        private enum ChangeType
        {
            Removed,
            AddedOrUpdated
        }
    }
}