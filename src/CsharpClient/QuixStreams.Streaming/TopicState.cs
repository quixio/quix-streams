using System;
using System.Collections;
using System.Collections.Generic;
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
        /// The unique name for this TopicState, which is a combination of the topic name and storage name.
        /// </summary>
        private readonly string name;
        
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
                    stateValueConverter = value => new StateValue(() => JsonConvert.SerializeObject(value, options)); // must be evaluated lazily as internal references can change whenever
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
            
            this.name = $"{topicName}-{storageName}";
            var fileStorage = new LocalFileStorage(this.name);
            this.state = new State.State(fileStorage);
            this.defaultValueFactory = defaultValueFactory ?? (s => throw new KeyNotFoundException("The specified key was not found and there was no default value factory set."));
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        {
            foreach (var value in this.state)
            {
                yield return new KeyValuePair<string, T>(value.Key, genericConverter(value.Value));
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
            this.state.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<string, T> item)
        {
            if (!state.TryGetValue(item.Key, out var val)) return false;
            var actual = genericConverter(val);
            return actual.Equals(item.Value);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }

            if (arrayIndex < 0 || arrayIndex > array.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            }

            if (array.Length - arrayIndex < this.Count)
            {
                throw new ArgumentException("The number of elements in the source collection is greater than the available space from the arrayIndex to the end of the destination array.");
            }

            foreach (var keyValue in this.state)
            {
                array[arrayIndex++] = new KeyValuePair<string, T>(keyValue.Key, genericConverter(keyValue.Value));
            }
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<string, T> item)
        {
            if (!this.Contains(item)) return false;
            return state.Remove(item.Key);
        }

        /// <inheritdoc/>
        public int Count => this.state.Count;
        
        /// <inheritdoc/>
        public bool IsReadOnly => false;
        
        /// <inheritdoc/>
        public void Add(string key, T value)
        {
            this.state.Add(key, stateValueConverter(value));
        }

        /// <inheritdoc/>
        public bool ContainsKey(string key)
        {
            return this.state.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool Remove(string key)
        {
            return this.state.Remove(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T value)
        {
            if (!state.TryGetValue(key, out var val))
            {
                value = default(T);
                return false;
            }

            value = genericConverter(val);
            return true;
        }

        /// <inheritdoc/>
        public T this[string key]
        {
            get
            {
                if (this.TryGetValue(key, out T val)) return val;
                val = this.defaultValueFactory(key);
                this.state[key] = stateValueConverter(val);
                return val;
            }
            set => this.state[key] = stateValueConverter(value);
        }

        /// <inheritdoc/>
        public ICollection<string> Keys => this.state.Keys;
        
        /// <inheritdoc/>
        public ICollection<T> Values => this.state.Values.Select(y => genericConverter(y)).ToArray();

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            this.state.Flush();
        }
    }
}