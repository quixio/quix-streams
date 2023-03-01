using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Provides additional context for the parameters
    /// </summary>
    public class ParameterDefinitions
    {
        /// <summary>
        /// Groups describing the hierarchy of parameters
        /// </summary>
        public List<ParameterGroupDefinition> ParameterGroups { get; set; }

        /// <summary>
        /// Parameters at root level
        /// </summary>
        public List<ParameterDefinition> Parameters { get; set; }
        
        /// <summary>
        /// Validate the data properties structures complies the specifications
        /// </summary>
        public void Validate()
        {
            // check if all groups have unique names at their level
            void ValidateGroupNames(List<ParameterGroupDefinition> groups, string path)
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
            ValidateGroupNames(this.ParameterGroups, "");

            // check if all parameters have unique Ids
            void ValidateGroupParameters(ParameterGroupDefinition group, List<string> existingIds)
            {
                if (group == null) return;
                ValidateParameters(group.Parameters, existingIds);
                ValidateGroupsParameters(group.ChildGroups, existingIds);
            }

            void ValidateGroupsParameters(List<ParameterGroupDefinition> groups, List<string> existingIds)
            {
                if (groups == null) return;
                foreach (var childGroup in groups)
                {
                    ValidateGroupParameters(childGroup, existingIds);
                }
            }

            void ValidateParameters(List<ParameterDefinition> parameters, List<string> existingIds)
            {
                if (parameters == null) return;
                foreach (var parameter in parameters)
                {
                    if (existingIds.Contains(parameter.Id))
                    {
                        if (string.IsNullOrWhiteSpace(parameter.Name)) throw new InvalidDataContractException("Parameter must have a unique Id");
                        throw new InvalidDataContractException($"Parameter must have a unique Id. Offending parameter: {parameter.Name}");
                    }
                    existingIds.Add(parameter.Id);
                }
            }
            var existingParameterIds = new List<string>();
            ValidateParameters(this.Parameters, existingParameterIds);
            ValidateGroupsParameters(this.ParameterGroups, existingParameterIds);
        }

    }
}