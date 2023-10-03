using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using QuixStreams.State;
using QuixStreams.State.Storage;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Represents a scalar storage of a value with a specific means to be persisted.
    /// </summary>
    public class StreamScalarState : IStreamState
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual value.
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
        
        /// <inheritdoc cref="IStreamState"/>
        public void Clear()
        {
            this.scalarState.Clear();
        }
        
        /// <summary>
        /// Gets or sets the value to the in-memory state.
        /// </summary>
        /// <returns>Returns the value</returns>
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
        
        /// <inheritdoc/>
        public void Dispose()
        {
            this.scalarState.Dispose();
        }
    }
    
    /// <summary>
    /// Represents a scalar storage of a value with a specific means to be persisted.
    /// </summary>
    /// <typeparam name="T">The type of values stored in the StreamState.</typeparam>
    public class StreamScalarState<T> : IStreamState
    {
        /// <summary>
        /// The underlying state storage for this StreamState, responsible for managing the actual value.
        /// </summary>
        private readonly State.ScalarState<T> scalarState;
        
        /// <summary>
        /// A function that returns a default value of type T when the value has not been set yet.
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
        /// Gets or sets the value to the in-memory state.
        /// </summary>
        /// <returns>Returns the value</returns>
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
        
        /// <inheritdoc cref="IStreamState"/>
        public void Clear()
        {
            this.scalarState.Clear();
        }

        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        public void Reset()
        {
            this.scalarState.Reset();
        }
        
        /// <inheritdoc/>
        public void Dispose()
        {
            this.scalarState.Dispose();
        }
    }
}