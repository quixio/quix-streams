using System;

namespace QuixStreams.Streaming.States
{
    /// <summary>
    /// Interface for a stream state
    /// </summary>
    public interface IStreamState: IDisposable
    {
        /// <summary>
        /// Raised immediately before a flush operation is performed.
        /// </summary>
        event EventHandler OnFlushing;

        /// <summary>
        /// Raised immediately after a flush operation is completed.
        /// </summary>
        event EventHandler OnFlushed;
        
        /// <summary>
        /// Clears the value of in-memory state and marks the state for clearing when flushed.
        /// </summary>
        void Clear();

        /// <summary>
        /// Flushes the changes made to the in-memory state to the specified storage.
        /// </summary>
        void Flush();

        /// <summary>
        /// Reset the state to before in-memory modifications
        /// </summary>
        void Reset();
    }
    
    /// <summary>
    /// Represents a method that returns the default value for a given key.
    /// </summary>
    /// <typeparam name="T">The type of the default value.</typeparam>
    /// <param name="missingStateKey">The name of the key being accessed.</param>
    /// <returns>The default value for the specified key.</returns>
    public delegate T StreamStateDefaultValueDelegate<out T>(string missingStateKey);
}