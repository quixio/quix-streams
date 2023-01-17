﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport.UnitTests.Helpers
{
    public class Passthrough : IOutput, IInput, ICanCommit
    {
        private readonly Func<Package, Task> callback;


        public Passthrough(Func<Package, Task> callback = null)
        {
            this.callback = callback;
        }

        public Func<Package, Task> OnNewPackage { get; set; }
        /// <inheritdoc/>
        public void Commit(TransportContext[] transportContexts)
        {
            this.OnCommitting?.Invoke(this, new OnCommittingEventArgs(transportContexts));
            this.OnCommitted?.Invoke(this, new OnCommittedEventArgs(transportContexts));
        }
        
        /// <inheritdoc/>
        public event EventHandler<OnCommittedEventArgs> OnCommitted;

        public event EventHandler<OnCommittingEventArgs> OnCommitting;

        public IEnumerable<TransportContext> FilterCommittedContexts(object state, IEnumerable<TransportContext> contextsToFilter)
        {
            return contextsToFilter;
        }

        public Task Send(Package package, CancellationToken cancellationToken = default)
        {
            return this.callback?.Invoke(package) ?? this.OnNewPackage?.Invoke(package);
        }
    }
}