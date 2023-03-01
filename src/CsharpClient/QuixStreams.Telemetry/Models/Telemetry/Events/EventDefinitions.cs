using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Provides additional context for the Event data
    /// </summary>
    public class EventDefinitions
    {
        /// <summary>
        /// Groups describing the hierarchy of events
        /// </summary>
        public List<EventGroupDefinition> EventGroups { get; set; }
        
        /// <summary>
        /// Events at root level
        /// </summary>
        public List<EventDefinition> Events { get; set; }

        /// <summary>
        /// Validate the data properties structures complies the specifications
        /// </summary>
        public void Validate()
        {
            // check if all groups have unique names at their level
            void ValidateGroupNames(List<EventGroupDefinition> groups, string path)
            {
                if (groups == null) return;
                var dupe = groups.GroupBy(x => x.Name).FirstOrDefault(x => x.Count() > 1)?.Key;
                if (!string.IsNullOrWhiteSpace(dupe))
                {
                    throw new InvalidDataContractException($"Groups must have a unique name. Offending group: {path}/{dupe}");
                }

                foreach (var group in groups)
                {
                    ValidateGroupNames(group.ChildGroups, $"{(path == "/" ? "" : path)}/{group.Name}");
                }
            }
            ValidateGroupNames(this.EventGroups, "");
            
            // check if all events have unique Ids 
            void ValidateGroupEvents(EventGroupDefinition group, List<string> existingIds)
            {
                if (group == null) return;
                ValidateEvents(group.Events, existingIds);
                ValidateGroupsEvents(group.ChildGroups, existingIds);
            }

            void ValidateGroupsEvents(List<EventGroupDefinition> groups, List<string> existingIds)
            {
                if (groups == null) return;
                foreach (var childGroup in groups)
                {
                    ValidateGroupEvents(childGroup, existingIds);
                }
            }

            void ValidateEvents(List<EventDefinition> events, List<string> existingIds)
            {
                if (events == null) return;
                foreach (var parameter in events)
                {
                    if (existingIds.Contains(parameter.Id))
                    {
                        if (string.IsNullOrWhiteSpace(parameter.Name)) throw new InvalidDataContractException("Event must have a unique Id");
                        throw new InvalidDataContractException($"Event must have a unique Id. Offending parameter: {parameter.Name}");
                    }
                    existingIds.Add(parameter.Id);
                }
            }
            var existingEventIds = new List<string>();
            ValidateEvents(this.Events, existingEventIds);
            ValidateGroupsEvents(this.EventGroups, existingEventIds);
        }

    }
}