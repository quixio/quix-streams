using System;
using System.Threading.Tasks;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Stands for an input/output connection point to receive and send data in a Stream Component
    /// Each Stream Component has an Input and an Output instances of this interface, where we are going to receive and send messages respectively.
    /// This Input and Output are also used to connect Components in a Pipeline, like when we "AddComponent" in a StreamPipeline class.
    /// </summary>
    public interface IIOComponentConnection
    {
        /// <summary>
        /// Send a non-typed message of <see cref="StreamPackage"/> through the component connection
        /// </summary>
        /// <param name="package">Non-typed message to send</param>
        Task Send(StreamPackage package);

        /// <summary>
        /// Send a typed message through the component connection
        /// </summary>
        /// <typeparam name="TModel">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <param name="model">Typed message to send</param>
        Task Send<TModel>(TModel model);

        /// <summary>
        /// Subscribes to all messages types received on this connection point
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receive a message</param>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IIOComponentConnection Subscribe(Func<StreamPackage, Task> onStreamPackage);

        /// <summary>
        /// Subscribes to a specific type of message received on this connection point
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receive a message</param>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IIOComponentConnection Subscribe<TModelType>(Func<TModelType, Task> onStreamPackage);

        /// <summary>
        /// Intercepts a specific type of messages received on this connection point.
        /// If a message is intercepted by the handler of this method, it is not going to be forwarded to any subscription method.
        /// Commonly used in a Component that wants to modify, delay or transform some types of messages in its logic.
        /// </summary>
        /// <typeparam name="TModelType"> Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <param name="onPackageHandler"></param>
        /// <returns>Same connection point that used this method, allowing to chain several intercepts in the same line of code</returns>
        IIOComponentConnection Intercept<TModelType>(Func<TModelType, Task> onPackageHandler);
        
        /// <summary>
        /// Subscribes to all messages types received on this connection point
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receive a message</param>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IIOComponentConnection Subscribe(Action<StreamPackage> onStreamPackage);

        /// <summary>
        /// Subscribes to a specific type of message received on this connection point
        /// </summary>
        /// <param name="onStreamPackage">Handler to use when receive a message</param>
        /// <typeparam name="TModelType">Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions in the same line of code</returns>
        IIOComponentConnection Subscribe<TModelType>(Action<TModelType> onStreamPackage);

        /// <summary>
        /// Intercepts a specific type of messages received on this connection point.
        /// If a message is intercepted by the handler of this method, it is not going to be forwarded to any subscription method.
        /// Commonly used in a Component that wants to modify, delay or transform some types of messages in its logic.
        /// </summary>
        /// <typeparam name="TModelType"> Type of the message registered in <see cref="CodecRegistry"/></typeparam>
        /// <param name="onPackageHandler"></param>
        /// <returns>Same connection point that used this method, allowing to chain several intercepts in the same line of code</returns>
        IIOComponentConnection Intercept<TModelType>(Action<TModelType> onPackageHandler);
    }
}
