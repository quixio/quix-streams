using System.Collections.Generic;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Describes data for a single event
    /// </summary>
    [ModelKey("EventData")]
    public class EventDataRaw
    {
        /// <summary>
        /// The timestamp of events in nanoseconds since unix epoch
        /// </summary>
        public long Timestamp { get; set; }
        
        /// <summary>
        /// Tags applied to the event
        /// </summary>
        public Dictionary<string, string> Tags { get; set; }
        
        /// <summary>
        /// The globally unique identifier of the event
        /// </summary>
        public string Id { get; set; }
        
        /// <summary>
        /// The value of the event
        /// </summary>
        public string Value { get; set; }
    }
}