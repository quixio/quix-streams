using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Implements the default input/output connection point to receive and send data in a Stream Component
    /// </summary>
    public sealed class IOComponentConnection : IIOComponentConnection
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<IOComponentConnection>();

        private readonly List<Func<StreamPackage, Task>> packageSubscriptions = new List<Func<StreamPackage, Task>>();
        private readonly Dictionary<Type, List<Func<object, Task>>> modelSubscriptions = new Dictionary<Type, List<Func<object, Task>>>();

        private readonly Dictionary<Type, Func<object, Task>> interceptors = new Dictionary<Type, Func<object, Task>>();
        
        /// <inheritdoc />
        public IIOComponentConnection Subscribe(Func<StreamPackage, Task> onStreamPackage)
        {
            this.logger.LogTrace("Registering generic package subscription (task)");
            this.packageSubscriptions.Add(onStreamPackage);

            return this;
        }

        /// <inheritdoc />
        public IIOComponentConnection Subscribe<TModelType>(Func<TModelType, Task> onStreamPackage)
        {
            this.logger.LogTrace("Registering subscription (task) for {0}", typeof(TModelType).Name);
            if (!this.modelSubscriptions.TryGetValue(typeof(TModelType), out var modelSubscriptionList))
            {
                modelSubscriptionList = new List<Func<object, Task>>();
                this.modelSubscriptions.Add(typeof(TModelType), modelSubscriptionList);
            }

            // Producing Action<object> delegates in subscription time to speed up generic Invokes
            modelSubscriptionList.Add(model => onStreamPackage.Invoke((TModelType)model));

            return this;
        }

        /// <inheritdoc />
        public Task Send(StreamPackage package)
        {
            return Process(package);
        }

        private async Task Process(StreamPackage package)
        {
            // Intercepts
            if (this.interceptors.TryGetValue(package.Type, out var onInterceptHandler))
            {
                this.logger.LogTrace("IOComponentConnection: {0} package has intercepts registered", package.Type.Name);
                await onInterceptHandler(package.Value);
                return;
            }

            // Model Subscriptions
            if (this.modelSubscriptions.TryGetValue(package.Type, out var modelSubscriptionList))
            {
                var value = package.Value;
                this.logger.LogTrace("IOComponentConnection: {0} package has {1} model subscriptions registered", package.Type.Name, modelSubscriptionList.Count);
                foreach (var subscription in modelSubscriptionList)
                {
                    var task = subscription.Invoke(value);
                    if (task != null)
                    {
                        await task;
                    }
                }
            }

            // Package Subscriptions
            foreach (var subscription in this.packageSubscriptions)
            {
                this.logger.LogTrace("IOComponentConnection: {0} package has {1} generic package subscription", package.Type.Name, packageSubscriptions.Count);
                var task = subscription?.Invoke(package);
                if (task != null) await task;
            }
        }
        /// <inheritdoc />
        public Task Send<TModel>(TModel model)
        {
            return this.Send(new StreamPackage(typeof(TModel), model));
        }
        

        /// <inheritdoc />
        public IIOComponentConnection Intercept<TModelType>(Func<TModelType, Task> onPackageHandler)
        {
            this.logger.LogTrace("Registering intercept (task) for {0}", typeof(TModelType).Name);
            // Producing Action<object> delegates in subscription time to speed up generic Invokes
            this.interceptors[typeof(TModelType)] = model => (onPackageHandler).Invoke((TModelType)model); 

            return this;
        }

        /// <inheritdoc />
        public IIOComponentConnection Subscribe(Action<StreamPackage> onStreamPackage)
        {
            this.logger.LogTrace("Registering generic package subscription (action)");
            this.Subscribe((package) =>
            {
                onStreamPackage(package);
                return Task.CompletedTask;
            });
            return this;
        }

        /// <inheritdoc />
        public IIOComponentConnection Subscribe<TModelType>(Action<TModelType> onStreamPackage)
        {
            this.logger.LogTrace("Registering subscription (action) for {0}", typeof(TModelType).Name);
            this.Subscribe<TModelType>((package) =>
            {
                onStreamPackage(package);
                return Task.CompletedTask;
            });
            return this;
        }

        /// <inheritdoc />
        public IIOComponentConnection Intercept<TModelType>(Action<TModelType> onPackageHandler)
        {
            this.logger.LogTrace("Registering intercept (action) for {0}", typeof(TModelType).Name);
            this.Intercept<TModelType>((package) =>
            {
                onPackageHandler(package);
                return Task.CompletedTask;
            });
            return this;
        }
    }
}
