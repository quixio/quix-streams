using System;
using System.Collections.Generic;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Transport.Fw
{
    /// <summary>
    /// Extensions for <see cref="ICanCommit"/> interface
    /// </summary>
    public static class ICanCommitExtensions
    {
        /// <summary>
        /// Helper method wrapping <see cref="ICanCommit.Commit"/>
        /// </summary>
        /// <param name="committer">The interface</param>
        /// <param name="transportContext">The transport context to commit</param>
        public static void Commit(this ICanCommit committer, TransportContext transportContext)
        {
            committer.Commit(new [] {transportContext});
        }    
    }

    /// <summary>
    /// Describes an interface for committing 
    /// </summary>
    public interface ICanCommit
    {
        /// <summary>
        /// Commits the transport context to the consumer.
        /// </summary>
        /// <param name="transportContexts">The transport context to commit</param>
        void Commit(TransportContext[] transportContexts);
        
        /// <summary>
        /// Event is raised when the transport context finished committing
        /// </summary>
        event EventHandler<OnCommittedEventArgs> OnCommitted;
        
        /// <summary>
        /// Event is raised when the transport context starts committing. It is not guaranteed to be raised if underlying broker initiates commit on its own
        /// </summary>
        event EventHandler<OnCommittingEventArgs> OnCommitting;
        
        /// <summary>
        /// Filters contexts that were affected by the commit.
        /// </summary>
        /// <param name="state">State raised by <see cref="OnCommitted"/></param>
        /// <param name="contextsToFilter">The contexts to filter</param>
        /// <returns>Contexts affected by the commit</returns>
        IEnumerable<TransportContext> FilterCommittedContexts(object state, IEnumerable<TransportContext> contextsToFilter);
    }

    /// <summary>
    /// Event args for <see cref="ICanCommit.OnCommitted"/>
    /// </summary>
    public class OnCommittedEventArgs : EventArgs
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="state">The state which describes the details of what has been committed</param>
        public OnCommittedEventArgs(object state)
        {
            this.State = state;
        }
        
        /// <summary>
        /// The state which describes the details of what has been committed
        /// </summary>
        public readonly object State;
    }
    
    /// <summary>
    /// Event args for <see cref="ICanCommit.OnCommitting"/>
    /// </summary>
    public class OnCommittingEventArgs : EventArgs
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="state">The state which describes the details of what has been committed</param>
        public OnCommittingEventArgs(object state)
        {
            this.State = state;
        }
        
        /// <summary>
        /// The state which describes the details of what has been committed
        /// </summary>
        public readonly object State;
    }

    /// <summary>
    /// Describes an interface for subscribing to an <see cref="ICanCommit"/>
    /// </summary>
    public interface ICanCommitSubscriber
    {
        /// <summary>
        /// Subscribes to a <see cref="ICanCommit"/>
        /// </summary>
        /// <param name="committer">The <see cref="ICanCommit"/> to subscribe to</param>
        void Subscribe(ICanCommit committer);
    }
}