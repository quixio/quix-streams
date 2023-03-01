using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.Streams.Telemetry.Models;

namespace Quix.Streams.Telemetry
{
    /// <summary>
    /// Stands for one Stream Process with its own StreamId and State (if it exists)
    /// Each Stream Process is composed by a list of Stream Components
    /// </summary>
    public interface IStreamProcess
    {
        /// <summary>
        /// Stream Id that has been assigned to the Stream process as a source Stream Id.
        /// If the Stream Process has been created as a result of messages read from a Message broker (Transport layer), this is the StreamId of all the incoming messages of the same stream.
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// List of metadata parameters read from the source of the data.
        /// The list of parameters depends on the implementation of the Transport layer, for instance for a Kafka broker we can find here the Topic information.
        /// </summary>
        Dictionary<string, string> SourceMetadata { get; set; }

        /// <summary>
        /// Chain a new component to the process.
        /// This method links the Output of the last component of the process to the Input of the added component.
        /// </summary>
        /// <param name="streamComponent">Component to chain to the process</param>
        /// <returns>Same stream process that used this method, allowing to chain several AddComponents calls in the same line of code</returns>
        IStreamProcess AddComponent(StreamComponent streamComponent);

        // /// <summary>
        // /// State related to the Stream process, tied to the container than runs the process and StreamId of the process.
        // /// This is the common place to save the state of all the components and the suitable to be used to recover the state of the process in case of failure.
        // /// </summary>
        // State State { get; }

        /// <summary>
        /// Send a non-typed message of <see cref="StreamPackage"/> to the first component of the process
        /// </summary>
        /// <param name="package">Non-typed message to send</param>
        Task Send(StreamPackage package);

        /// <summary>
        /// Send a typed message to the first component of the process
        /// </summary>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <param name="model">Typed message to send</param>
        Task Send<TModelType>(TModelType model);

        /// <summary>
        /// Subscribes to all message types received to the Output of the last component of the process.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <returns>Same stream process that used this method, allowing to chain several Subscribe calls in the same line of code</returns>
        IStreamProcess Subscribe(Func<IStreamProcess, StreamPackage, Task> onStreamPackage);

        /// <summary>
        /// Subscribes to a specific type of message received to the Output of the last component of the process.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IStreamProcess Subscribe<TModelType>(Func<IStreamProcess, TModelType, Task> onStreamPackage);

        /// <summary>
        /// Subscribes to all message types received to the Output of the last component of the process.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <returns>Same stream process that used this method, allowing to chain several Subscribe calls in the same line of code</returns>
        IStreamProcess Subscribe(Action<IStreamProcess, StreamPackage> onStreamPackage);

        /// <summary>
        /// Subscribes to a specific type of message received to the Output of the last component of the process.
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receiving a message</param>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IStreamProcess Subscribe<TModelType>(Action<IStreamProcess, TModelType> onStreamPackage);
        
        /// <summary>
        /// Close and dispose all the components of the process.
        /// </summary>
        void Close();

        
        /// <summary>
        /// Invoked when the Stream process close starts
        /// </summary>
        event Action OnClosing;
        
        /// <summary>
        /// Invoked when the Stream process has closed
        /// </summary>
        event Action OnClosed;
    }
}