using System;
using System.Collections;
using System.Collections.Generic;

namespace Quix.Sdk.Streaming.Utils
{
    /// <summary>
    /// ReadOnlyDictionary which returns the default value instead of exception when the key is not found via indexer
    /// </summary>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <typeparam name="TValue">The type of the value</typeparam>
    internal class DefaultReadOnlyDictionary<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>
    {
        private readonly IDictionary<TKey, TValue> sourceDictionary;
        private readonly TValue defaultValue;

        public DefaultReadOnlyDictionary(IDictionary<TKey, TValue> sourceDictionary, TValue defaultValue = default)
        {
            this.sourceDictionary = sourceDictionary ?? throw new ArgumentNullException(nameof(sourceDictionary));
            this.defaultValue = defaultValue;
        }
        
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return this.sourceDictionary.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count => this.sourceDictionary.Count;
        
        public bool ContainsKey(TKey key)
        {
            return this.sourceDictionary.ContainsKey(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return this.sourceDictionary.TryGetValue(key, out value);
        }

        public TValue this[TKey key] => this.sourceDictionary.TryGetValue(key, out var value) ? value : defaultValue;

        public IEnumerable<TKey> Keys => this.sourceDictionary.Keys;
        public IEnumerable<TValue> Values => this.sourceDictionary.Values;
    }
}