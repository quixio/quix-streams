using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry.Managers
{
    /// <summary>
    /// Helper class to manage ParameterDefinitions generation complexity (Parameters Root and Parameter Groups)
    /// </summary>
    public class ParameterDefinitionsManager
    {
        private readonly object modificationLock = new object();
        private readonly Dictionary<string, Dictionary<string, ParameterDefinition>> parameterDefinitions
            = new Dictionary<string, Dictionary<string, ParameterDefinition>>();

        /// <summary>
        /// Initializes a new instance of <see cref="ParameterDefinitionsManager"/>
        /// </summary>
        public ParameterDefinitionsManager()
        {
            this.GenerateLocations("/");
        }

        /// <summary>
        /// Add a new <see cref="ParameterDefinition"/> at the specified location
        /// </summary>
        /// <param name="parameterDefinition"><see cref="ParameterDefinition"/> to add</param>
        /// <param name="location">Location where the <see cref="ParameterDefinition"/> should be located at (ex: /car/chassis)</param>
        public void AddDefinition(ParameterDefinition parameterDefinition, string location)
        {
            location = ReformatLocation(location);
            lock (modificationLock)
            {
                this.GenerateLocations(location);
                this.parameterDefinitions[location][parameterDefinition.Id] = parameterDefinition;
            }
        }

        /// <summary>
        /// Generate the <see cref="ParameterDefinitions"/> tree based on the added individual <see cref="ParameterDefinition"/> objects
        /// </summary>
        /// <returns>Parameter Definitions tree object</returns>
        public ParameterDefinitions GenerateParameterDefinitions()
        {
            lock (modificationLock)
            {
                var definitions = new ParameterDefinitions()
                {
                    Parameters = this.parameterDefinitions["/"].Select(kv => kv.Value).ToList(),
                    ParameterGroups = this.GenerateGroups("/"),
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
        /// Generate internal <see cref="ParameterDefinitions"/> tree objects, based on provided location
        /// </summary>
        /// <param name="targetLocation">Location to generate <see cref="ParameterDefinitions"/> tree from</param>
        public void GenerateLocations(string targetLocation)
        {
            lock (modificationLock)
            {
                targetLocation = ReformatLocation(targetLocation);
                if (!this.parameterDefinitions.TryGetValue(targetLocation, out var _))
                {
                    this.parameterDefinitions[targetLocation] = new Dictionary<string, ParameterDefinition>();
                }

                var index = Math.Max(targetLocation.LastIndexOf('/'), targetLocation.LastIndexOf('\\'));
                if (index > 0)
                {
                    targetLocation = targetLocation.Substring(0, index);
                    this.GenerateLocations(targetLocation);
                }
            }
        }

        private List<ParameterGroupDefinition> GenerateGroups(string parentLocation)
        {
            var parentLevel = this.GetLevel(parentLocation);
            var subgroupPaths = parentLocation == "/" ? "/" : parentLocation + "/";
            var groups = this.parameterDefinitions.Select(kv => kv.Key).Where(loc => parentLevel == this.GetLevel(loc) - 1 && loc.StartsWith(subgroupPaths)).ToList();

            var generated = groups.Select(loc => new ParameterGroupDefinition()
            {
                Name = this.GetName(loc),
                Parameters = this.parameterDefinitions[loc].Select(kv => kv.Value).ToList(),
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
