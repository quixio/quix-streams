using System;
using System.Collections.Generic;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Transport.Fw
{
    /// <summary>
    /// Describes interface for publishing information about revocation
    /// </summary>
    public interface IRevocationPublisher
    {
        /// <summary>
        /// Raised when losing access to source depending on implementation
        /// Argument is the state which describes what is being revoked, depending on implementation
        /// </summary>
        event EventHandler<OnRevokingEventArgs> OnRevoking;

        /// <summary>
        /// Raised when lost access to source depending on implementation
        /// Argument is the state which describes what got revoked, depending on implementation
        /// </summary>
        event EventHandler<OnRevokedEventArgs> OnRevoked;

        /// <summary>
        /// Filters contexts affected by the revocation.
        /// </summary>
        /// <param name="state">State raised as argument of <see cref="OnRevoking"/> or <seealso cref="OnRevoked"/></param>
        /// <param name="contexts">The contexts to filter</param>
        /// <returns>Contexts affected by the state</returns>
        IEnumerable<TransportContext> FilterRevokedContexts(object state, IEnumerable<TransportContext> contexts);
    }

    /// <summary>
    /// OnRevoking event arguments
    /// </summary>
    public class OnRevokingEventArgs : EventArgs
    {
        /// <summary>
        /// State information of the revoking operation
        /// </summary>
        public object State { get; set; }
    }

    /// <summary>
    /// OnRevoked event arguments
    /// </summary>
    public class OnRevokedEventArgs : EventArgs
    {
        /// <summary>
        /// State information of the revoke operation
        /// </summary>
        public object State { get; set; }
    }
    
    /// <summary>
    /// Describes an interface for subscribing to a <see cref="IRevocationPublisher"/>
    /// </summary>
    public interface IRevocationSubscriber
    {
        /// <summary>
        /// Subscribes to a <see cref="IRevocationPublisher"/>
        /// </summary>
        /// <param name="revocationPublisher">The <see cref="IRevocationPublisher"/> to subscribe to</param>
        void Subscribe(IRevocationPublisher revocationPublisher);
    }
}