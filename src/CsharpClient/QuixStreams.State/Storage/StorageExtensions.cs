using System;
using System.Threading.Tasks;
using QuixStreams.State.Serializers;

namespace QuixStreams.State.Storage
{
    /// <summary>
    /// Extension methods for Storage classes
    /// </summary>
    public static class StorageExtensions
    {

        /// <summary>
        /// Set long value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        public static void Set(this IStateStorage stateStorage, string key, long value)
        {
            stateStorage.SetAsync(key, value).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Set long value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        /// <returns>Awaitable task</returns>
        public static Task SetAsync(this IStateStorage stateStorage, string key, long value)
        {
            return stateStorage.SetAsync(key, new StateValue(value));
        }

        /// <summary>
        /// Set double value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        public static void Set(this IStateStorage stateStorage, string key, double value)
        {
            stateStorage.SetAsync(key, value).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Set double value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        /// <returns>Awaitable task</returns>
        public static Task SetAsync(this IStateStorage stateStorage, string key, double value)
        {
            return stateStorage.SetAsync(key, new StateValue(value));
        }

        /// <summary>
        /// Set string value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        public static void Set(this IStateStorage stateStorage, string key, string value)
        {
            stateStorage.SetAsync(key, value).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Set string value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        /// <returns>Awaitable task</returns>
        public static Task SetAsync(this IStateStorage stateStorage, string key, string value)
        {
            return stateStorage.SetAsync(key, new StateValue(value));
        }

        /// <summary>
        /// Set binary value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        public static void Set(this IStateStorage stateStorage, string key, byte[] value)
        {
            stateStorage.SetAsync(key, value).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Set binary value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        /// <returns>Awaitable task</returns>
        public static Task SetAsync(this IStateStorage stateStorage, string key, byte[] value)
        {
            return stateStorage.SetAsync(key, new StateValue(value));
        }

        /// <summary>
        /// Set bool value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        public static void Set(this IStateStorage stateStorage, string key, bool value)
        {
            stateStorage.SetAsync(key, value).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Set bool value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        /// <returns>Awaitable task</returns>
        public static Task SetAsync(this IStateStorage stateStorage, string key, bool value)
        {
            return stateStorage.SetAsync(key, new StateValue(value));
        }


        /// <summary>
        /// Set boxed StateValue value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        public static void Set(this IStateStorage stateStorage, string key, StateValue value)
        {
            stateStorage.SetAsync(key, value).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Set boxed StateValue value into the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <param name="value">Value to be stored</param>
        /// <returns>Awaitable task</returns>
        public static Task SetAsync(this IStateStorage stateStorage, string key, StateValue value)
        {
            return stateStorage.SaveRaw(key, ByteValueSerializer.Serialize(value));
        }

        /// <summary>
        /// Get boxed StateValue value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="NotSupportedException">If underlying value is not <see cref="StateValue"/></exception>
        /// <returns>State boxed value</returns>
        public static StateValue Get(this IStateStorage stateStorage, string key)
        {
            return stateStorage.GetAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get boxed StateValue value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="NotSupportedException">If underlying value is not <see cref="StateValue"/></exception>
        /// <returns>Awaitable task for <see cref="StateValue"/>State value boxed value</returns>
        public static async Task<StateValue> GetAsync(this IStateStorage stateStorage, string key)
        {
            return ByteValueSerializer.Deserialize(await stateStorage.LoadRaw(key));
        }

        /// <summary>
        /// Get bool value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not double</exception>
        public static double GetDouble(this IStateStorage stateStorage, string key)
        {
            return stateStorage.GetDoubleAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get bool value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not double</exception>
        /// <returns>Awaitable task for double value</returns>
        public static async Task<double> GetDoubleAsync(this IStateStorage stateStorage, string key)
        {
            return (await stateStorage.GetAsync(key)).DoubleValue;
        }

        /// <summary>
        /// Get string value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not string</exception>
        public static string GetString(this IStateStorage stateStorage, string key)
        {
            return stateStorage.GetStringAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get string value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not string</exception>
        /// <returns>Awaitable task for string value</returns>
        public static async Task<string> GetStringAsync(this IStateStorage stateStorage, string key)
        {
            return (await stateStorage.GetAsync(key)).StringValue;
        }

        /// <summary>
        /// Get bool value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not boolean</exception>
        public static bool GetBool(this IStateStorage stateStorage, string key)
        {
            return stateStorage.GetBoolAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get bool value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not boolean</exception>
        /// <returns>Awaitable task for boolean value</returns>
        public static async Task<bool> GetBoolAsync(this IStateStorage stateStorage, string key)
        {
            return (await stateStorage.GetAsync(key)).BoolValue;
        }

        /// <summary>
        /// Get long value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not long</exception>
        public static long GetLong(this IStateStorage stateStorage, string key)
        {
            return stateStorage.GetLongAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get long value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not long</exception>
        /// <returns>Awaitable task for long value</returns>
        public static async Task<long> GetLongAsync(this IStateStorage stateStorage, string key)
        {
            return (await stateStorage.GetAsync(key)).LongValue;
        }

        /// <summary>
        /// Get binary value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not byte[]</exception>
        public static byte[] GetBinary(this IStateStorage stateStorage, string key)
        {
            return stateStorage.GetBinaryAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get binary value from the storage
        /// </summary>
        /// <param name="stateStorage">Storage to be set</param>
        /// <param name="key">Key of the value</param>
        /// <exception cref="InvalidCastException">If value is not byte[]</exception>
        /// <returns>Awaitable task for byte[] value</returns>
        public static async Task<byte[]> GetBinaryAsync(this IStateStorage stateStorage, string key)
        {
            return (await stateStorage.GetAsync(key)).BinaryValue;
        }

        /// <summary>
        /// Remove key from the storage
        /// </summary>
        /// <param name="stateStorage">Storage instance</param>
        /// <param name="key">Key to be removed</param>
        public static void Remove(this IStateStorage stateStorage, string key)
        {
            stateStorage.RemoveAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Check if the storage contains the key
        /// </summary>
        /// <param name="stateStorage">Storage instance</param>
        /// <param name="key">Key to be checked</param>
        public static bool ContainsKey(this IStateStorage stateStorage, string key)
        {
            return stateStorage.ContainsKeyAsync(key).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get list of all keys in the storage
        /// </summary>
        /// <param name="stateStorage">Storage instance</param>
        public static string[] GetAllKeys(this IStateStorage stateStorage)
        {
            return stateStorage.GetAllKeysAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Clear content (all keys) of the storage
        /// </summary>
        /// <param name="stateStorage">Storage instance</param>
        public static void Clear(this IStateStorage stateStorage)
        {
            stateStorage.ClearAsync().GetAwaiter().GetResult();
        }
    }
}
