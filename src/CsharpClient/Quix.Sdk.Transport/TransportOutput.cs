using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport
{
    /// <summary>
    /// A prebuilt pipeline, which deserializes and merges the package's output by the specified <see cref="IOutput"/>
    /// </summary>
    public class TransportOutput : IOutput, ICanCommit, IRevocationPublisher
    {
        private readonly Action onClose = () => { };
        private readonly Action<TransportContext[]> onCommit = (context) => { };
        private readonly Func<object, IEnumerable<TransportContext>, IEnumerable<TransportContext>>
            onFilterCommittedContexts = (cm, co) => Array.Empty<TransportContext>(); 
        private readonly Func<object, IEnumerable<TransportContext>, IEnumerable<TransportContext>> contextFilterByState = (state, context) => context;

        /// <summary>
        /// Initializes a new instance of <see cref="TransportOutput"/>, which listens to the specified <see cref="IOutput"/>
        /// </summary>
        /// <param name="output">The output to listen to</param>
        public TransportOutput(IOutput output) : this(output, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportOutput"/>, which listens to the specified <see cref="IOutput"/>
        /// </summary>
        /// <param name="output">The output to listen to</param>
        /// <param name="configureOptions"></param>
        public TransportOutput(IOutput output, Action<TransportOutputOptions> configureOptions)
        {
            if (output == null) throw new ArgumentNullException(nameof(output));
            var options = new TransportOutputOptions();
            configureOptions?.Invoke(options);

            // output -> merger -> deserializer -> commitModifier -> raise
            var outputsAndInputs = new List<object>();
            outputsAndInputs.Add(output);

            var byteMerger = new ByteMerger(new MergeBuffer());
            var merger = new ByteMergingModifier(byteMerger);
            outputsAndInputs.Add(merger);

            var deserializer = new DeserializingModifier();
            outputsAndInputs.Add(deserializer);
            if (options.CommitOptions?.AutoCommitEnabled ?? false)
            {
                var commitModifier = new CommitModifier(options.CommitOptions);
                outputsAndInputs.Add(commitModifier);
                onClose = () => commitModifier.Close();
            }
            
            // Now that we have the modifier, lets connect them up
            var previous = outputsAndInputs[0];
            for (var index = 1; index < outputsAndInputs.Count; index++)
            {
                var modifier = outputsAndInputs[index];
                ((IOutput) previous).OnNewPackage = p => ((IInput) modifier).Send(p) ?? Task.CompletedTask;
                previous = modifier;
            }

            // Connect last output to TransportOutput (this class)
            ((IOutput) previous).OnNewPackage =  p=> this.OnNewPackage?.Invoke(p) ?? Task.CompletedTask;
            
            // Hook up committing modifiers from front (output) to back (this)
            ICanCommit previousCanCommitModifier = null;
            for (var index = 0; index < outputsAndInputs.Count; index++)
            {
                var modifier = outputsAndInputs[index];
                if (previousCanCommitModifier != null)
                {
                    if (modifier is ICanCommitSubscriber subscriber)
                    {
                        subscriber.Subscribe(previousCanCommitModifier);
                    }
                }

                if (modifier is ICanCommit committingModifier) previousCanCommitModifier = committingModifier;
            }
            
            // Connect last committing modifiers to TransportOutput (this class)
            if (previousCanCommitModifier != null)
            {
                this.onCommit = (context) => { previousCanCommitModifier.Commit(context); };
                this.onFilterCommittedContexts = (state, contexts) => previousCanCommitModifier.FilterCommittedContexts(state, contexts);
                previousCanCommitModifier.OnCommitted += (sender, args) => this.OnCommitted?.Invoke(sender, args);
                previousCanCommitModifier.OnCommitting += (sender, args) => this.OnCommitting?.Invoke(sender, args);
            }
            
            // Hook up modifiers implementing IRevocation.... from front (output) to back (this)
            IRevocationPublisher previousRevocationPublisher = null;
            for (var index = 0; index < outputsAndInputs.Count; index++)
            {
                var modifier = outputsAndInputs[index];
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
            
            // Connect last IRevocation... to TransportOutput (this class)
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
        /// Close transport output
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
    /// Transport output options
    /// </summary>
    public class TransportOutputOptions
    {
        /// <summary>
        /// Auto commit options
        /// </summary>
        public CommitOptions CommitOptions { get; set; } = new CommitOptions();
    }
}