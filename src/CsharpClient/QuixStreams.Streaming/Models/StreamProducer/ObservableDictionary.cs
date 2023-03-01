using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Linq;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// Observable dictionary based on https://codereview.stackexchange.com/questions/202663/simple-observabledictionary-implementation
    /// </summary>
    public class ObservableDictionary<TKey, TValue> : IDictionary, IDictionary<TKey, TValue>, IReadOnlyDictionary<TKey, TValue>, INotifyCollectionChanged, INotifyPropertyChanged
    {
        private Dictionary<TKey, TValue> Dictionary { get; }

        #region Constants (standart constants for collection/dictionary)

        private const string CountString = "Count";
        private const string IndexerName = "Item[]";
        private const string KeysName = "Keys";
        private const string ValuesName = "Values";

        #endregion

        #region .ctor

        /// <inheritdoc cref="System.Collections.Generic.Dictionary&lt;TKey,TValue&gt;" />
        public ObservableDictionary()
        {
            Dictionary = new Dictionary<TKey, TValue>();
        }

        /// <inheritdoc cref="System.Collections.Generic.Dictionary&lt;TKey,TValue&gt;" />
        public ObservableDictionary(IDictionary<TKey, TValue> dictionary)
        {
            Dictionary = new Dictionary<TKey, TValue>(dictionary);
        }

        /// <inheritdoc cref="System.Collections.Generic.Dictionary&lt;TKey,TValue&gt;" />
        public ObservableDictionary(IEqualityComparer<TKey> comparer)
        {
            Dictionary = new Dictionary<TKey, TValue>(comparer);
        }

        /// <inheritdoc cref="System.Collections.Generic.Dictionary&lt;TKey,TValue&gt;" />
        public ObservableDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
        {
            Dictionary = new Dictionary<TKey, TValue>(dictionary, comparer);
        }

        /// <inheritdoc cref="System.Collections.Generic.Dictionary&lt;TKey,TValue&gt;" />
        public ObservableDictionary(int capacity, IEqualityComparer<TKey> comparer)
        {
            Dictionary = new Dictionary<TKey, TValue>(capacity, comparer);
        }

        /// <inheritdoc cref="System.Collections.Generic.Dictionary&lt;TKey,TValue&gt;" />
        public ObservableDictionary(int capacity)
        {
            Dictionary = new Dictionary<TKey, TValue>(capacity);
        }

        #endregion

        #region INotifyCollectionChanged and INotifyPropertyChanged

        /// <inheritdoc/>
        public event NotifyCollectionChangedEventHandler CollectionChanged;

        /// <inheritdoc/>
        public event PropertyChangedEventHandler PropertyChanged;

        #endregion

        #region IDictionary<TKey, TValue> Implementation

        /// <inheritdoc cref="IDictionary{TKey,TValue}.this" />
        public TValue this[TKey key]
        {
            get
            {
                return Dictionary[key];
            }
            set
            {
                InsertObject(
                    key: key,
                    value: value,
                    appendMode: AppendMode.Replace,
                    oldValue: out var oldItem);

                if (oldItem != null)
                {
                    OnCollectionChanged(
                        action: NotifyCollectionChangedAction.Replace,
                        newItem: new KeyValuePair<TKey, TValue>(key, value),
                        oldItem: new KeyValuePair<TKey, TValue>(key, oldItem));
                }
                else
                {
                    OnCollectionChanged(
                        action: NotifyCollectionChangedAction.Add,
                        changedItem: new KeyValuePair<TKey, TValue>(key, value));
                }
            }
        }

        /// <inheritdoc/>
        public ICollection<TKey> Keys => Dictionary.Keys;

        /// <inheritdoc/>
        public ICollection<TValue> Values => Dictionary.Values;

        /// <inheritdoc cref="ICollection.Count" />
        public int Count => Dictionary.Count;

        /// <inheritdoc cref="IDictionary.IsReadOnly" />
        public bool IsReadOnly => ((IDictionary)Dictionary).IsReadOnly;
        
        #region IDictionary

        /// <inheritdoc/>
        ICollection IDictionary.Values => Dictionary.Values;

        /// <inheritdoc/>
        ICollection IDictionary.Keys => Dictionary.Keys;

        /// <inheritdoc/>
        public void CopyTo(Array array, int index)
        {
            ((IDictionary)Dictionary).CopyTo(array, index);
        }
        
        /// <inheritdoc/>
        public bool IsSynchronized => ((IDictionary)Dictionary).IsSynchronized;
        
        /// <inheritdoc/>
        public object SyncRoot => ((IDictionary)Dictionary).SyncRoot;
        
        /// <inheritdoc/>
        public object this[object key]
        {
            get => ((IDictionary)Dictionary)[key];
            set => ((IDictionary)Dictionary)[key] = value;
        }
        
        /// <inheritdoc/>
        public void Add(object key, object value)
        {
            ((IDictionary)Dictionary).Add(key, value);
        }
        
        /// <inheritdoc/>
        public bool Contains(object key)
        {
            return ((IDictionary)Dictionary).Contains(key);
        }

        /// <inheritdoc/>
        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return ((IDictionary)Dictionary).GetEnumerator();
        }

        
        /// <inheritdoc/>
        public void Remove(object key)
        {
            ((IDictionary)Dictionary).Remove(key);
        }

        /// <inheritdoc/>
        public bool IsFixedSize => ((IDictionary)Dictionary).IsFixedSize;

        #endregion

        /// <inheritdoc/>
        public void Add(TKey key, TValue value)
        {
            InsertObject(
                key: key,
                value: value,
                appendMode: AppendMode.Add);

            OnCollectionChanged(
                action: NotifyCollectionChangedAction.Add,
                changedItem: new KeyValuePair<TKey, TValue>(key, value));
        }

        /// <inheritdoc/>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            InsertObject(
                key: item.Key,
                value: item.Value,
                appendMode: AppendMode.Add);

            OnCollectionChanged(
                action: NotifyCollectionChangedAction.Add,
                changedItem: new KeyValuePair<TKey, TValue>(item.Key, item.Value));
        }

        /// <inheritdoc/>
        public void Clear()
        {
            if (!Dictionary.Any())
            {
                return;
            }

            var removedItems = new List<KeyValuePair<TKey, TValue>>(Dictionary.ToList());
            Dictionary.Clear();
            OnCollectionChanged(
                action: NotifyCollectionChangedAction.Reset,
                newItems: null,
                oldItems: removedItems);
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return Dictionary.Contains(item);
        }

        /// <inheritdoc/>
        public bool ContainsKey(TKey key)
        {
            return Dictionary.ContainsKey(key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            ((IDictionary<TKey, TValue>)Dictionary).CopyTo(array: array, arrayIndex: arrayIndex);
        }

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return Dictionary.GetEnumerator();
        }

        /// <inheritdoc/>
        public bool Remove(TKey key)
        {
            if (Dictionary.TryGetValue(key, out var value))
            {
                Dictionary.Remove(key);
                OnCollectionChanged(
                    action: NotifyCollectionChangedAction.Remove,
                    changedItem: new KeyValuePair<TKey, TValue>(key, value));
                return true;
            }

            return false;
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            if (((IDictionary<TKey, TValue>)Dictionary).Remove(item))
            {
                OnCollectionChanged(
                    action: NotifyCollectionChangedAction.Remove,
                    changedItem: item);
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public bool TryGetValue(TKey key, out TValue value)
        {
            return Dictionary.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Dictionary.GetEnumerator();
        }

        #endregion

        #region IReadOnlyDictionary

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Dictionary.Keys;
        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Dictionary.Values;

        #endregion

        #region ObservableDictionary inner methods

        private void InsertObject(TKey key, TValue value, AppendMode appendMode)
        {
            InsertObject(key, value, appendMode, out var _);
        }

        private void InsertObject(TKey key, TValue value, AppendMode appendMode, out TValue oldValue)
        {
            oldValue = default;

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (Dictionary.TryGetValue(key, out var item))
            {
                if (appendMode == AppendMode.Add)
                {
                    throw new ArgumentException("Item with the same key has already been added");
                }

                if (Equals(item, value))
                {
                    return;
                }

                Dictionary[key] = value;
                oldValue = item;
            }
            else
            {
                Dictionary[key] = value;
            }
        }

        private void OnPropertyChanged()
        {
            OnPropertyChanged(CountString);
            OnPropertyChanged(IndexerName);
            OnPropertyChanged(KeysName);
            OnPropertyChanged(ValuesName);
        }

        private void OnPropertyChanged(string propertyName)
        {
            if (string.IsNullOrWhiteSpace(propertyName))
            {
                OnPropertyChanged();
            }

            var handler = PropertyChanged;
            handler?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private void OnCollectionChanged()
        {
            OnPropertyChanged();
            var handler = CollectionChanged;
            handler?.Invoke(
                this, new NotifyCollectionChangedEventArgs(
                    action: NotifyCollectionChangedAction.Reset));
        }

        private void OnCollectionChanged(NotifyCollectionChangedAction action, KeyValuePair<TKey, TValue> changedItem)
        {
            OnPropertyChanged();
            var handler = CollectionChanged;
            handler?.Invoke(
                this, new NotifyCollectionChangedEventArgs(
                    action: action,
                    changedItem: changedItem));
        }

        private void OnCollectionChanged(NotifyCollectionChangedAction action, KeyValuePair<TKey, TValue> newItem, KeyValuePair<TKey, TValue> oldItem)
        {
            OnPropertyChanged();
            var handler = CollectionChanged;
            handler?.Invoke(
                this, new NotifyCollectionChangedEventArgs(
                    action: action,
                    newItem: newItem,
                    oldItem: oldItem));
        }

        private void OnCollectionChanged(NotifyCollectionChangedAction action, IList newItems)
        {
            OnPropertyChanged();
            var handler = CollectionChanged;
            handler?.Invoke(
                this, new NotifyCollectionChangedEventArgs(
                    action: action,
                    changedItems: newItems));
        }

        private void OnCollectionChanged(NotifyCollectionChangedAction action, IList newItems, IList oldItems)
        {
            OnPropertyChanged();
            var handler = CollectionChanged;
            handler?.Invoke(
                this, new NotifyCollectionChangedEventArgs(
                    action: action,
                    newItems: newItems,
                    oldItems: oldItems));
        }

        #endregion

        internal enum AppendMode
        {
            Add,
            Replace
        }
    }
}

