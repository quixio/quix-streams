using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using QuixStreams.State;
using QuixStreams.State.Storage.FileStorage.LocalFileStorage;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    /// <typeparam name="T">The type of values stored in the TopicState.</typeparam>
    public class TopicState<T> : IDictionary<string, T>
    {
        /// <summary>
        /// Returns whether the type is passed by reference
        /// </summary>
        private readonly bool IsTypeByRef = typeof(T).IsByRef;
        
        /// <summary>
        /// The unique name for this TopicState, which is a combination of the topic name and storage name.
        /// </summary>
        private readonly string name;
        
        /// <summary>
        /// Indicates whether the state should be cleared before flushing the changes.
        /// </summary>
        private bool clearBeforeFlush = false;
        
        /// <summary>
        /// The underlying state storage for this TopicState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.State state;

        /// <summary>
        /// A function that converts a StateValue to the desired value of type T, using appropriate conversion logic based on the type of T.
        /// </summary>
        private readonly Func<StateValue, T> genericConverter;
        
        /// <summary>
        /// A function that converts a value of type T to a StateValue, using appropriate conversion logic based on the type of T.
        /// </summary>
        private readonly Func<T, StateValue> stateValueConverter;
        
        /// <summary>
        /// A function that takes a string key and returns a default value of type T when the key is not found in the state storage.
        /// </summary>
        private readonly Func<string, T> defaultValueFactory;

        /// <summary>
        /// The in-memory cache to avoid conversions between State and T whenever possible
        /// </summary>
        private readonly IDictionary<string, T> inMemoryCache = new Dictionary<string, T>();
        
        /// <summary>
        /// Represents the changes made to the in-memory state, tracking additions, updates, and removals.
        /// </summary>
        private readonly IDictionary<string, ChangeType> changes = new Dictionary<string, ChangeType>();
        
        /// <summary>
        /// Returns whether the cache keys are case sensitive
        /// </summary>
        private bool IsCaseSensitive => this.state.IsCaseSensitive; 

        /// <summary>
        /// Initializes a new instance of the TopicState class.
        /// </summary>
        /// <param name="topicName">The topic name for this TopicState.</param>
        /// <param name="storageName">The storage name for this TopicState.</param>
        /// <param name="defaultValueFactory">A function that takes a string key and returns a default value of type T when the key is not found in the state</param>
        public TopicState(string topicName, string storageName, Func<string, T> defaultValueFactory)
        {
            if (string.IsNullOrWhiteSpace(topicName)) throw new ArgumentNullException(nameof(topicName));
            if (string.IsNullOrWhiteSpace(storageName)) throw new ArgumentNullException(nameof(storageName));

            var type = typeof(T);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    throw new ArgumentException($"{Type.GetTypeCode(type)} is not supported by {nameof(TopicState<T>)}.");
                case TypeCode.Object:
                    var options = new JsonSerializerSettings()
                    {
                        Formatting = Formatting.None
                    };
                    genericConverter = value => JsonConvert.DeserializeObject<T>(value.StringValue);
                    stateValueConverter = value => new StateValue(JsonConvert.SerializeObject(value, options)); // must be evaluated lazily as internal references can change whenever
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
                    stateValueConverter = value => new StateValue((byte[])(object)BitConverter.GetBytes((ulong)(object)value));
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
            
            this.name = $"{topicName}{Path.DirectorySeparatorChar}{storageName}";
            var fileStorage = new LocalFileStorage($"state{Path.DirectorySeparatorChar}{Path.DirectorySeparatorChar}{this.name}");
            this.state = new State.State(fileStorage);
            this.defaultValueFactory = defaultValueFactory ?? (s => throw new KeyNotFoundException("The specified key was not found and there was no default value factory set."));
            foreach (var pair in this.state)
            {
                this.inMemoryCache[pair.Key] = genericConverter(pair.Value);
            }
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        {
            return this.inMemoryCache.GetEnumerator();
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
            this.inMemoryCache.Clear();
            this.clearBeforeFlush = true;
            this.changes.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, T> item)
        {
            return this.ContainsKey(item.Key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            this.inMemoryCache.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, T> item)
        {
            return this.Remove(item.Key);
        }

        /// <inheritdoc/>
        public int Count => this.inMemoryCache.Count;
        
        /// <inheritdoc/>
        public bool IsReadOnly => false;
        
        /// <inheritdoc/>
        public void Add(string key, T value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            inMemoryCache.Add(key, value);
            this.changes[key] = ChangeType.AddedOrUpdated;
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            return this.inMemoryCache.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            this.changes[key] = ChangeType.Removed;
            return this.inMemoryCache.Remove(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T value)
        {
            if (!this.IsCaseSensitive) key = key.ToLower();
            return inMemoryCache.TryGetValue(key, out value);
        }

        /// <inheritdoc/>
        public T this[string key]
        {
            get
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                if (this.TryGetValue(key, out T val)) return val;
                val = this.defaultValueFactory(key);
                this.inMemoryCache[key] = val;
                this.changes[key] = ChangeType.AddedOrUpdated;
                return val;
            }
            set
            {
                if (!this.IsCaseSensitive) key = key.ToLower();
                this.changes[key] = ChangeType.AddedOrUpdated;
                this.inMemoryCache[key] = value;
            }
        }

        /// <inheritdoc/>
        public ICollection<string> Keys => this.inMemoryCache.Keys;

        /// <inheritdoc/>
        public ICollection<T> Values => this.inMemoryCache.Values;

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            if (this.clearBeforeFlush)
            {
                this.state.Clear();
                this.clearBeforeFlush = false;
            }

            // Check if the current instance is type by reference
            if (this.IsTypeByRef)
            {
                // If it is, loop through each change in the list of changes
                foreach (var changeType in changes)
                {
                    // For any change that has a value of "Removed", remove the corresponding key from the internal state
                    if (changeType.Value == ChangeType.Removed)
                    {
                        this.state.Remove(changeType.Key);
                    }
                }

                // After all removals are complete, loop through each item in an in-memory cache
                foreach (var pair in this.inMemoryCache)
                {
                    // Update the internal state by applying a state value converter to each value and storing the result in the state with the same key
                    // this is necessary because reference types might have changed without this instance knowing
                    this.state[pair.Key] = stateValueConverter(pair.Value);
                }
            }
            else
            {
                // If the instance is not type by reference, loop through each change in the list of changes
                foreach (var changeType in changes)
                {
                    // For any change that has a value of "Removed", remove the corresponding key from the internal state
                    if (changeType.Value == ChangeType.Removed)
                    {
                        this.state.Remove(changeType.Key);
                    }
                    else
                    {
                        // For any change that is not "Removed", look up the corresponding value in the in-memory cache
                        // Apply the state value converter to the value and store the result in the internal state with the same key
                        this.state[changeType.Key] = stateValueConverter(inMemoryCache[changeType.Key]);
                    }
                }
            }
            
            this.changes.Clear();

            this.state.Flush();
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