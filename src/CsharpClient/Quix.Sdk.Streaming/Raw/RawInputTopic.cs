using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Streaming.Raw
{
    /// <summary>
    /// Topic class to read incoming raw messages (capable to read non-sdk messages)
    /// </summary>
    public class RawInputTopic: IRawInputTopic, IDisposable
    {
        private KafkaOutput kafkaOutput;
        private bool connectionStarted = false;

        EventHandler<Exception> _errorHandler;
        bool errorHandlerRegistered = false;

        /// <inheritdoc />
        public event EventHandler<RawMessage> OnMessageRead;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;

        /// <inheritdoc />
        public event EventHandler<Exception> OnErrorOccurred
        {
            add {
                _errorHandler += value;
                if (_errorHandler != null && !errorHandlerRegistered)
                {
                    //automatic attaching the handler when noone someone starts listening to the event
                    // internally causing to stop logging messages instead of start throwing them
                    this.kafkaOutput.ErrorOccurred += InternalErrorHandler;
                    errorHandlerRegistered = true;
                }
            }
            remove { 
                _errorHandler -= value;
                if (_errorHandler == null && errorHandlerRegistered)
                {
                    //automatic detaching of the handler when noone is listening to the event
                    // internally causing to start logging messages instead of throwing them
                    this.kafkaOutput.ErrorOccurred -= InternalErrorHandler;
                    errorHandlerRegistered = false;
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of <see cref="RawInputTopic"/>
        /// </summary>
        /// <param name="brokerAddress">Address of Kafka cluster.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="brokerProperties">Additional broker properties</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        public RawInputTopic(string brokerAddress, string topicName, string consumerGroup, Dictionary<string, string> brokerProperties = null, AutoOffsetReset? autoOffset = null)
        {
            brokerProperties = brokerProperties ?? new Dictionary<string, string>();
            if (!brokerProperties.ContainsKey("fetch.message.max.bytes")) brokerProperties["fetch.message.max.bytes"] = "20480";

            var subscriberConfiguration = new SubscriberConfiguration(brokerAddress, consumerGroup, brokerProperties);

            if (autoOffset != null)
            {
                subscriberConfiguration.AutoOffsetReset = autoOffset?.ConvertToKafka();
            }

            //disable quix-custom keep alive messages because they can interfere with the received data since we dont have any protocol running this over
            subscriberConfiguration.CheckForKeepAlivePackets = false;

            var topicConfiguration = new OutputTopicConfiguration(topicName);
            this.kafkaOutput = new KafkaOutput(subscriberConfiguration, topicConfiguration);
        }

        /// <inheritdoc />
        public void StartReading()
        {
            if(connectionStarted)
            {
                //throw exception for double starting
                throw new InvalidOperationException("Cannot call StartReading twice");
            }

            kafkaOutput.OnNewPackage = async package =>
            {
                byte[] message = (byte[])package.Value.Value;

                Lazy < ReadOnlyDictionary<string, string> > meta = new Lazy<ReadOnlyDictionary<string, string>>(() =>
                   {
                       Dictionary<string, string> vals = new Dictionary<string, string>();
                       foreach(var el in package.TransportContext)
                       {
                           var value = el.Value;
                           if (value == null) {
                               vals[el.Key] = "";
                           } else {
                               vals[el.Key] = value.ToString();
                           }
                       }
                       return new ReadOnlyDictionary<string, string>(vals);
                   });
                this.OnMessageRead?.Invoke(this, new RawMessage(package.GetKey(), message, meta));
            };

            kafkaOutput.Open();
            connectionStarted = true;
        }

        /// <summary>
        /// Internal handler for handing Error event from the kafkaOutput
        /// </summary>
        /// <param name="source"></param>
        /// <param name="ex"></param>
        void InternalErrorHandler(object source, Exception ex)
        {
            this._errorHandler?.Invoke(this, ex);
        }


        /// <inheritdoc />
        public void Dispose()
        {
            this.kafkaOutput?.Dispose();
            this.OnDisposed?.Invoke(this, EventArgs.Empty);
        }

    }
}