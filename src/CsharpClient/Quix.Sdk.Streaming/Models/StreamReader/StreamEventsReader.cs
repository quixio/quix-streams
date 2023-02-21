using System;
using System.Collections.Generic;
using System.Linq;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models.StreamReader
{
    /// <summary>
    /// Helper class for read <see cref="EventDefinitions"/> and <see cref="EventData"/>
    /// </summary>
    public class StreamEventsReader : IDisposable
    {
        private readonly IInputTopic topic;
        private readonly IStreamReaderInternal streamReader;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamParametersReader"/>
        /// </summary>
        /// <param name="topic">The topic the stream to what this reader belongs to</param>
        /// <param name="streamReader">Stream reader owner</param>
        internal StreamEventsReader(IInputTopic topic, IStreamReaderInternal streamReader)
        {
            this.topic = topic;
            this.streamReader = streamReader;

            this.streamReader.OnEventDefinitionsChanged += OnEventDefinitionsChangedHandler;

            this.streamReader.OnEventData += OnEventDataHandler;

        }

        private void OnEventDataHandler(IStreamReader sender, Process.Models.EventDataRaw eventDataRaw)
        {
            var data = new EventData(eventDataRaw);

            this.OnRead?.Invoke(this, new EventDataReadEventArgs(this.topic, this.streamReader, data));
        }

        private void OnEventDefinitionsChangedHandler(IStreamReader sender, EventDefinitions eventDefinitions)
        {
            this.LoadFromProcessDefinitions(eventDefinitions);

            this.OnDefinitionsChanged?.Invoke(this, new EventDefinitionsChangedEventArgs(this.topic, this.streamReader));
        }

        /// <summary>
        /// Raised when an events data package is read for the stream
        /// </summary>
        public event EventHandler<EventDataReadEventArgs> OnRead;

        /// <summary>
        /// Raised when the even definitions have changed for the stream.
        /// See <see cref="Definitions"/> for the latest set of event definitions
        /// </summary>
        public event EventHandler<EventDefinitionsChangedEventArgs> OnDefinitionsChanged;

        /// <summary>
        /// Gets the latest set of event definitions
        /// </summary>
        public IList<EventDefinition> Definitions { get; private set; } 

        private void LoadFromProcessDefinitions(Process.Models.EventDefinitions definitions)
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

        private List<EventDefinition> ConvertEventDefinitions(List<Process.Models.EventDefinition> eventDefinitions, string location)
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

        private List<EventDefinition> ConvertGroupEventDefinitions(List<Process.Models.EventGroupDefinition> eventGroupDefinitions, string location)
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
            this.streamReader.OnEventDefinitionsChanged -= OnEventDefinitionsChangedHandler;

            this.streamReader.OnEventData -= OnEventDataHandler;
        }
    }

    public class EventDefinitionsChangedEventArgs
    {
        public EventDefinitionsChangedEventArgs(IInputTopic topic, IStreamReader reader)
        {
            this.Topic = topic;
            this.Stream = reader;
        }
        
                
        public IInputTopic Topic { get; }
        public IStreamReader Stream { get; }
    }
    
    public class EventDataReadEventArgs
    {
        public EventDataReadEventArgs(IInputTopic topic, IStreamReader reader, EventData data)
        {
            this.Topic = topic;
            this.Stream = reader;
            this.Data = data;
        }
        
        public IInputTopic Topic { get; }
        public IStreamReader Stream { get; }
        public EventData Data { get; }
    }
}
