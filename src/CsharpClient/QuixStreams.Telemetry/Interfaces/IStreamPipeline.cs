using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Stands for one Stream pipeline with its own StreamId and State (if it exists)
    /// Each Stream pipeline is composed by a list of Stream Components
    /// </summary>
    public interface IStreamPipeline
    {
        /// <summary>
        /// Stream Id that has been assigned to the Stream pipeline as a source Stream Id.
        /// If the Stream pipeline has been created as a result of messages read from a Message broker (Transport layer), this is the StreamId of all the incoming messages of the same stream.
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// Chain a new component to the pipeline.
        /// This method links the Output of the last component of the pipeline to the Input of the added component.
        /// </summary>
        /// <param name="streamComponent">Component to chain to the pipeline</param>
        /// <returns>Same stream pipeline that used this method, allowing to chain several AddComponents calls in the same line of code</returns>
        IStreamPipeline AddComponent(StreamComponent streamComponent);

        // /// <summary>
        // /// State related to the Stream pipeline, tied to the container than runs the pipeline and StreamId of the pipeline.
        // /// This is the common place to save the state of all the components and the suitable to be used to recover the state of the pipeline in case of failure.
        // /// </summary>
        // State State { get; }

        /// <summary>
        /// Send a non-typed message of <see cref="StreamPackage"/> to the first component of the pipeline
        /// </summary>
        /// <param name="package">Non-typed message to send</param>
        Task Send(StreamPackage package);

        /// <summary>
        /// Send a typed message to the first component of the pipeline
        /// </summary>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <param name="model">Typed message to send</param>
        Task Send<TModelType>(TModelType model);

        /// <summary>
        /// Subscribes to all message types received to the Output of the last component of the pipeline.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <returns>Same stream pipeline that used this method, allowing to chain several Subscribe calls in the same line of code</returns>
        IStreamPipeline Subscribe(Func<IStreamPipeline, StreamPackage, Task> onStreamPackage);

        /// <summary>
        /// Subscribes to a specific type of message received to the Output of the last component of the pipeline.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IStreamPipeline Subscribe<TModelType>(Func<IStreamPipeline, TModelType, Task> onStreamPackage);

        /// <summary>
        /// Subscribes to all message types received to the Output of the last component of the pipeline.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <returns>Same stream pipeline that used this method, allowing to chain several Subscribe calls in the same line of code</returns>
        IStreamPipeline Subscribe(Action<IStreamPipeline, StreamPackage> onStreamPackage);

        /// <summary>
        /// Subscribes to a specific type of message received to the Output of the last component of the pipeline.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IStreamPipeline Subscribe<TModelType>(Action<IStreamPipeline, TModelType> onStreamPackage);
        
        /// <summary>
        /// Close and dispose all the components of the pipeline.
        /// </summary>
        void Close();

        
        /// <summary>
        /// Invoked when the Stream pipeline close starts
        /// </summary>
        event Action OnClosing;
        
        /// <summary>
        /// Invoked when the Stream pipeline has closed
        /// </summary>
        event Action OnClosed;
    }
}