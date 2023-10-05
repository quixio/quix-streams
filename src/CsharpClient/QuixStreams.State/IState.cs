using System;

namespace QuixStreams.State
{
    /// <summary>
    /// Interface for a state
    /// </summary>
    public interface IState: IDisposable
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
        /// Sets the value of in-memory state to null and marks the state for clearing when flushed.
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
}