using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using QuixStreams.State;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    public class StreamScalarState : IStreamState
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.ScalarState scalarState;

        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the StreamState class.
        /// </summary>
        /// <param name="storage">The storage the stream state is going to use as underlying storage</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal StreamScalarState(IStateStorage storage, ILoggerFactory loggerFactory)
        {
            this.scalarState = new State.ScalarState(storage, loggerFactory);
        }

        
        /// <inheritdoc cref="IDictionary.Clear" />
        public void Clear()
        {
            this.scalarState.Clear();
        }
        
        /// <summary>
        /// fill me
        /// </summary>
        public StateValue Value
        {
            get => scalarState.Value;
            set => scalarState.Value = value;
        }

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.scalarState.Flush();
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }
        
        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            this.scalarState.Reset();
        }
    }
    
    /// <summary>
    /// Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    /// </summary>
    /// <typeparam name="T">The type of values stored in the StreamState.</typeparam>
    public class StreamScalarState<T> : IStreamState
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual key-value pairs.
        /// </summary>
        private readonly State.ScalarState<T> scalarState;
        /// <summary>
        /// Returns whether the cache keys are case-sensitive
        /// </summary>
        
        /// <summary>
        /// A function that takes a string key and returns a default value of type T when the key is not found in the state storage.
        /// </summary>
        private readonly StreamStateDefaultValueDelegate<T> defaultValueFactory;

        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        public event EventHandler OnFlushing;
        
        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        public event EventHandler OnFlushed;

        /// <summary>
        /// Initializes a new instance of the StreamState class.
        /// </summary>
        /// <param name="storage">The storage the stream state is going to use as underlying storage</param>
        /// <param name="defaultValueFactory">A function that returns a default value of type T when the value has not been set yet</param>
        /// <param name="loggerFactory">The logger factory to use</param>
        internal StreamScalarState(IStateStorage storage, StreamStateDefaultValueDelegate<T> defaultValueFactory, ILoggerFactory loggerFactory)
        {
            this.scalarState = new State.ScalarState<T>(storage, loggerFactory);
            this.defaultValueFactory = defaultValueFactory ?? (s => throw new KeyNotFoundException("The specified key was not found and there was no default value factory set."));
        }
        
        /// <summary>
        /// fill me
        /// </summary>
        public T Value
        {
            get
            {
                if (this.scalarState.Value != null) return this.scalarState.Value;
                var val = this.defaultValueFactory(string.Empty);
                this.scalarState.Value = val;
                return val;
            }
            set => this.scalarState.Value = value;
        }
        
        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        public void Flush()
        {
            OnFlushing?.Invoke(this, EventArgs.Empty);
            this.scalarState.Flush();
            OnFlushed?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            this.scalarState.Reset();
        }
    }
}