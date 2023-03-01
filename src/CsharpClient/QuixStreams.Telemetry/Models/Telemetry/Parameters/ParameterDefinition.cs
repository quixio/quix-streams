using System;

namespace QuixStreams.Telemetry.Models
{

    /// <summary>
    /// Describes additional context for the parameter
    /// </summary>
    public class ParameterDefinition
    {
        private string id;

        /// <summary>
        /// Parameter Id. Must be unique within the stream.
        /// This must match the parameter id you use to send the data
        /// </summary>
        public string Id
        {
            get => id;
            set
            {
                if (string.IsNullOrWhiteSpace(value)) throw new ArgumentOutOfRangeException(nameof(Id), "Parameter must have a unique Id");
                if (value.IndexOfAny(new char[] {'/', '\\'}) > -1)
                {
                    throw new ArgumentOutOfRangeException(nameof(Id), "Parameter Id must not contain the following characters: /\\");
                }
                id = value;
            }
        }

        /// <summary>
        /// Human friendly display name of the parameter
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Description of the parameter
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// Minimum value of the parameter
        /// </summary>
        public double? MinimumValue { get; set; }
        
        /// <summary>
        /// Maximum value of the parameter
        /// </summary>
        public double? MaximumValue { get; set; }

        /// <summary>
        /// Unit of the parameter 
        /// </summary>
        public string Unit { get; set; }

        /// <summary>
        /// The formatting to apply on the value for display purposes
        /// </summary>
        public string Format { get; set; }

        /// <summary>
        /// Optional field for any custom properties that do not exist on the parameter.
        /// For example this could be a json string, describing the optimal value range of this parameter
        /// </summary>
        public string CustomProperties { get; set; }
    }
}