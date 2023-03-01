using System;
using System.Collections.Generic;
using System.Linq;
using Quix.Streams.Telemetry.Models;

namespace Quix.Streams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Helper class for read <see cref="EventDefinitions"/> and <see cref="EventData"/>
    /// </summary>
    public class StreamEventsConsumer : IDisposable
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamTimeseriesConsumer"/>
        /// </summary>
        /// <param name="topicConsumer">The topic the stream to what this reader belongs to</param>
        /// <param name="streamConsumer">Stream reader owner</param>
        internal StreamEventsConsumer(ITopicConsumer topicConsumer, IStreamConsumerInternal streamConsumer)
        {
            this.topicConsumer = topicConsumer;
            this.streamConsumer = streamConsumer;

            this.streamConsumer.OnEventDefinitionsChanged += OnEventDefinitionsChangedHandler;

            this.streamConsumer.OnEventData += OnEventDataHandler;

        }

        private void OnEventDataHandler(IStreamConsumer sender, Telemetry.Models.EventDataRaw eventDataRaw)
        {
            var data = new EventData(eventDataRaw);

            this.OnDataReceived?.Invoke(this, new EventDataReadEventArgs(this.topicConsumer, this.streamConsumer, data));
        }

        private void OnEventDefinitionsChangedHandler(IStreamConsumer sender, EventDefinitions eventDefinitions)
        {
            this.LoadFromProcessDefinitions(eventDefinitions);

            this.OnDefinitionsChanged?.Invoke(this, new EventDefinitionsChangedEventArgs(this.topicConsumer, this.streamConsumer));
        }

        /// <summary>
        /// Raised when an events data package is received for the stream
        /// </summary>
        public event EventHandler<EventDataReadEventArgs> OnDataReceived;

        /// <summary>
        /// Raised when the even definitions have changed for the stream.
        /// See <see cref="Definitions"/> for the latest set of event definitions
        /// </summary>
        public event EventHandler<EventDefinitionsChangedEventArgs> OnDefinitionsChanged;

        /// <summary>
        /// Gets the latest set of event definitions
        /// </summary>
        public IList<EventDefinition> Definitions { get; private set; } 

        private void LoadFromProcessDefinitions(Telemetry.Models.EventDefinitions definitions)
        {
            // Create a new list instead of modifying publicly available list to avoid threading issues like
            // user iterating the list then us changing it during it
            var defs = new List<EventDefinition>();
            
            if (definitions.Events != null) 
                this.ConvertEventDefinitions(definitions.Events, "").ForEach(d => defs.Add(d));
            if (definitions.EventGroups != null)
                this.ConvertGroupEventDefinitions(definitions.EventGroups, "").ForEach(d => defs.Add(d));

            this.Definitions = defs;
        }

        private List<EventDefinition> ConvertEventDefinitions(List<Telemetry.Models.EventDefinition> eventDefinitions, string location)
        {
            var result = eventDefinitions.Select(d => new EventDefinition
            {
                Id = d.Id,
                Name = d.Name,
                Description = d.Description,
                CustomProperties = d.CustomProperties,
                Level = d.Level,
                Location = location
            }).ToList();

            return result;
        }

        private List<EventDefinition> ConvertGroupEventDefinitions(List<Telemetry.Models.EventGroupDefinition> eventGroupDefinitions, string location)
        {
            var result = new List<EventDefinition>();

            foreach (var group in eventGroupDefinitions)
            {
                if (group.Events != null)
                    this.ConvertEventDefinitions(group.Events, location + "/" + group.Name).ForEach(d => result.Add(d));
                if (group.ChildGroups != null)
                    this.ConvertGroupEventDefinitions(group.ChildGroups, location + "/" + group.Name).ForEach(d => result.Add(d));
            }

            return result;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamConsumer.OnEventDefinitionsChanged -= OnEventDefinitionsChangedHandler;

            this.streamConsumer.OnEventData -= OnEventDataHandler;
        }
    }

    public class EventDefinitionsChangedEventArgs
    {
        public EventDefinitionsChangedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
        }
        
                
        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
    }
    
    public class EventDataReadEventArgs
    {
        public EventDataReadEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer, EventData data)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
            this.Data = data;
        }
        
        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
        public EventData Data { get; }
    }
}
