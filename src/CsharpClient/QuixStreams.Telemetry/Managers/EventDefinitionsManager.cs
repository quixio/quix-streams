using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry.Managers
{
    /// <summary>
    /// Helper class to manage EventDefinitions generation complexity (Events Root and Event Groups)
    /// </summary>
    public class EventDefinitionsManager
    {
        private readonly object modificationLock = new object();
        private readonly Dictionary<string, Dictionary<string, EventDefinition>> eventsDefinitions
            = new Dictionary<string, Dictionary<string, EventDefinition>>();

        /// <summary>
        /// Initializes a new instance of <see cref="EventDefinitionsManager"/>
        /// </summary>
        public EventDefinitionsManager()
        {
            this.GenerateLocations("/");
        }

        /// <summary>
        /// Add a new <see cref="EventDefinition"/> at the specified location
        /// </summary>
        /// <param name="eventDefinition"><see cref="EventDefinition"/> to add</param>
        /// <param name="location">Location where the <see cref="EventDefinition"/> should be located at (ex: /car/chassis)</param>
        public void AddDefinition(EventDefinition eventDefinition, string location)
        {
            location = ReformatLocation(location);
            lock (modificationLock)
            {
                this.GenerateLocations(location);
                this.eventsDefinitions[location][eventDefinition.Id] = eventDefinition;
            }
        }

        /// <summary>
        /// Generate the <see cref="EventDefinitions"/> tree based on the added individual <see cref="EventDefinition"/> objects
        /// </summary>
        /// <returns><see cref="EventDefinitions"/> tree object</returns>
        public EventDefinitions GenerateEventDefinitions()
        {
            lock (modificationLock)
            {
                var definitions = new EventDefinitions()
                {
                    Events = this.eventsDefinitions["/"].Select(kv => kv.Value).ToList(),
                    EventGroups = this.GenerateGroups("/"),
                };
                return definitions;
            }
        }
        
        /// <summary>
        /// Formats location according to expected standard.
        /// This means leading /, no trailing /
        /// </summary>
        /// <param name="location">Location to format</param>
        /// <returns>Reformatted location</returns>
        public string ReformatLocation(string location)
        {
            location = location ?? "";
            location = location.Trim('/');
            location = $"/{location}"; // ensure only one starting / and no trailing /
            return location;
        }

        /// <summary>
        /// Generate internal <see cref="EventDefinitions"/> tree objects, based on provided location
        /// </summary>
        /// <param name="targetLocation">Location to generate <see cref="EventDefinitions"/> tree from</param>
        public void GenerateLocations(string targetLocation)
        {
            lock (modificationLock)
            {
                targetLocation = ReformatLocation(targetLocation);
                if (!this.eventsDefinitions.TryGetValue(targetLocation, out var _))
                {
                    this.eventsDefinitions[targetLocation] = new Dictionary<string, EventDefinition>();
                }

                var index = Math.Max(targetLocation.LastIndexOf('/'), targetLocation.LastIndexOf('\\'));
                if (index > 0)
                {
                    targetLocation = targetLocation.Substring(0, index);
                    this.GenerateLocations(targetLocation);
                }
            }
        }

        private List<EventGroupDefinition> GenerateGroups(string parentLocation)
        {
            var parentLevel = this.GetLevel(parentLocation);
            var subgroupPaths = parentLocation == "/" ? "/" : parentLocation + "/";
            var groups = this.eventsDefinitions.Select(kv => kv.Key).Where(loc => parentLevel == this.GetLevel(loc) - 1 && loc.StartsWith(subgroupPaths)).ToList();

            var generated = groups.Select(loc => new EventGroupDefinition()
            {
                Name = this.GetName(loc),
                Events = this.eventsDefinitions[loc].Select(kv => kv.Value).ToList(),
                ChildGroups = this.GenerateGroups(loc)
            }
            ).ToList();

            return generated;
        }

        private int GetLevel(string loc)
        {
            if (loc.Length == 1) return 0;
            var count = loc.Count(c => c == '/' || c == '\\');
            return count;

        }

        private string GetName(string loc)
        {
            var index = Math.Max(loc.LastIndexOf('/'), loc.LastIndexOf('\\'));

            if (index < 0) return string.Empty;

            loc = loc.Substring(index + 1, loc.Length - index - 1);
            return loc;
        }
    }
}
