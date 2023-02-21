using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport
{
    /// <summary>
    /// A prebuilt pipeline, which deserializes and merges the packages and passes to the specified <see cref="IConsumer"/>
    /// </summary>
    public class TransportConsumer : IConsumer, ICanCommit, IRevocationPublisher
    {
        private readonly Action onClose = () => { };
        private readonly Action<TransportContext[]> onCommit = (context) => { };
        private readonly Func<object, IEnumerable<TransportContext>, IEnumerable<TransportContext>>
            onFilterCommittedContexts = (cm, co) => Array.Empty<TransportContext>(); 
        private readonly Func<object, IEnumerable<TransportContext>, IEnumerable<TransportContext>> contextFilterByState = (state, context) => context;

        /// <summary>
        /// Initializes a new instance of <see cref="TransportConsumer"/>, which listens to the specified <see cref="IConsumer"/>
        /// </summary>
        /// <param name="consumer">The consumer to listen to</param>
        public TransportConsumer(IConsumer consumer) : this(consumer, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportConsumer"/>, which listens to the specified <see cref="IConsumer"/>
        /// </summary>
        /// <param name="consumer">The consumer to listen to</param>
        /// <param name="configureOptions"></param>
        public TransportConsumer(IConsumer consumer, Action<TransportConsumerOptions> configureOptions)
        {
            if (consumer == null) throw new ArgumentNullException(nameof(consumer));
            var options = new TransportConsumerOptions();
            configureOptions?.Invoke(options);

            // consumer -> merger -> deserializer -> commitModifier -> raise
            var pipeline = new List<object>();
            pipeline.Add(consumer);

            var byteMerger = new ByteMerger(new MergeBuffer());
            var merger = new ByteMergingModifier(byteMerger);
            pipeline.Add(merger);

            var deserializer = new DeserializingModifier();
            pipeline.Add(deserializer);
            if (options.CommitOptions?.AutoCommitEnabled ?? false)
            {
                var commitModifier = new CommitModifier(options.CommitOptions);
                pipeline.Add(commitModifier);
                onClose = () => commitModifier.Close();
            }
            
            // Now that we have the modifier, lets connect them up
            var previous = pipeline[0];
            for (var index = 1; index < pipeline.Count; index++)
            {
                var modifier = pipeline[index];
                ((IConsumer) previous).OnNewPackage = p => ((IProducer) modifier).Publish(p) ?? Task.CompletedTask;
                previous = modifier;
            }

            // Connect last consumer to TransportConsumer (this class)
            ((IConsumer) previous).OnNewPackage =  p=> this.OnNewPackage?.Invoke(p) ?? Task.CompletedTask;
            
            // Hook up committing modifiers from front (consumer) to back (this)
            ICanCommit previousCanCommitModifier = null;
            for (var index = 0; index < pipeline.Count; index++)
            {
                var modifier = pipeline[index];
                if (previousCanCommitModifier != null)
                {
                    if (modifier is ICanCommitSubscriber subscriber)
                    {
                        subscriber.Subscribe(previousCanCommitModifier);
                    }
                }

                if (modifier is ICanCommit committingModifier) previousCanCommitModifier = committingModifier;
            }
            
            // Connect last committing modifiers to TransportConsumer (this class)
            if (previousCanCommitModifier != null)
            {
                this.onCommit = (context) => { previousCanCommitModifier.Commit(context); };
                this.onFilterCommittedContexts = (state, contexts) => previousCanCommitModifier.FilterCommittedContexts(state, contexts);
                previousCanCommitModifier.OnCommitted += (sender, args) => this.OnCommitted?.Invoke(sender, args);
                previousCanCommitModifier.OnCommitting += (sender, args) => this.OnCommitting?.Invoke(sender, args);
            }
            
            // Hook up modifiers implementing IRevocation.... from front (consumer) to back (this)
            IRevocationPublisher previousRevocationPublisher = null;
            for (var index = 0; index < pipeline.Count; index++)
            {
                var modifier = pipeline[index];
                if (previousRevocationPublisher != null)
                {
                    // If we have previous notification modifier then try to hook up callbacks to it. Without previous notification modifier
                    // there would be nothing to hook up to.
                    if (modifier is IRevocationSubscriber subscriber)
                    {
                        subscriber.Subscribe(previousRevocationPublisher);
                    }
                }
                
                // assign current as previous for next loop
                if (modifier is IRevocationPublisher revocationPublisher) previousRevocationPublisher = revocationPublisher;
            }
            
            // Connect last IRevocation... to TransportConsumer (this class)
            if (previousRevocationPublisher != null)
            {
                this.contextFilterByState = previousRevocationPublisher.FilterRevokedContexts;
                previousRevocationPublisher.OnRevoked += (sender, args) => this.OnRevoked?.Invoke(this, args);
                previousRevocationPublisher.OnRevoking += (sender, args) => this.OnRevoking?.Invoke(this, args);
            }
            // Done hooking up modifiers implementing IRevocation
        }
        
        /// <inheritdoc/>
        public void Commit(TransportContext[] transportContexts)
        {
            this.onCommit(transportContexts);
        }
        
        /// <inheritdoc/>
        public IEnumerable<TransportContext> FilterCommittedContexts(object state, IEnumerable<TransportContext> contextsToFilter)
        {
            return this.onFilterCommittedContexts(state, contextsToFilter) ?? Array.Empty<TransportContext>();
        }
        
        /// <inheritdoc/>
        public event EventHandler<OnCommittedEventArgs> OnCommitted;
        
        /// <inheritdoc/>
        public event EventHandler<OnCommittingEventArgs> OnCommitting;
        
        /// <summary>
        /// The callback that is used when new package is available
        /// </summary>
        public Func<Package, Task> OnNewPackage { get; set; }

        /// <summary>
        /// Close transport consumer
        /// </summary>
        public void Close()
        {
            this.onClose();
        }

        /// <inheritdoc/>
        public event EventHandler<OnRevokingEventArgs> OnRevoking;
        
        /// <inheritdoc/>
        public event EventHandler<OnRevokedEventArgs> OnRevoked;

        /// <inheritdoc/>
        public IEnumerable<TransportContext> FilterRevokedContexts(object state, IEnumerable<TransportContext> contexts)
        {
            return contextFilterByState(state, contexts);
        }
    }

    /// <summary>
    /// Transport consumer options
    /// </summary>
    public class TransportConsumerOptions
    {
        /// <summary>
        /// Auto commit options
        /// </summary>
        public CommitOptions CommitOptions { get; set; } = new CommitOptions();
    }
}