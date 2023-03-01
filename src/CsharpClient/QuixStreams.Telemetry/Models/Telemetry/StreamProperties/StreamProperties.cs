using System;
using System.Collections.Generic;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Provides additional context for the stream
    /// </summary>
    public class StreamProperties
    {
        /// <summary>
        /// The human friendly name of the stream
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Specify location of the stream in data catalogue. 
        /// For example: /cars/ai/carA/.
        /// </summary>
        public string Location { get; set; }

        /// <summary>
        /// Additional metadata for the stream.
        /// </summary>
        public Dictionary<string, string> Metadata { get; set; }

        /// <summary>
        /// The ids of streams this stream is derived from.
        /// </summary>
        public List<string> Parents { get; set; }

        /// <summary>
        /// Indicates the time when data was originally recorded.
        /// This can be different than the time the data is streamed.
        /// </summary>
        public DateTime? TimeOfRecording { get; set; }

        /// <summary>
        /// Returns the hash of the content
        /// </summary>
        /// <returns>Hash of the content</returns>
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Location != null ? Location.GetHashCode() : 0);
                if (Metadata == null)
                {
                    hashCode = (hashCode * 397) ^ 0;
                }
                else
                {
                    foreach (var kpair in Metadata)
                    {
                        hashCode = (hashCode * 397) ^ (kpair.Key != null ? kpair.Key.GetHashCode() : 0);
                        hashCode = (hashCode * 397) ^ (kpair.Value != null ? kpair.Value.GetHashCode() : 0);
                    }
                }
                
                if (Parents == null)
                {
                    hashCode = (hashCode * 397) ^ 0;
                }
                else
                {
                    foreach (var parent in Parents)
                    {
                        hashCode = (hashCode * 397) ^ (parent != null ? parent.GetHashCode() : 0);
                    }
                }
                
                hashCode = (hashCode * 397) ^ TimeOfRecording.GetHashCode();
                return hashCode;
            }
        }
    }
}
