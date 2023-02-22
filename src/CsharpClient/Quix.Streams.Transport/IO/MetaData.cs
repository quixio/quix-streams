using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Quix.Streams.Transport.IO
{
    /// <summary>
    /// This is read only meta data context meant to use for providing extra context for the value
    /// </summary>
    public sealed class MetaData : ReadOnlyDictionary<string, string>
    {
        /// <summary>
        /// Empty metadata instance
        /// </summary>
        public static readonly MetaData Empty = new MetaData(new ReadOnlyDictionary<string, string>(new Dictionary<string, string>()));
        
        /// <summary>
        /// Initializes a new instance of <see cref="MetaData" />
        /// </summary>
        /// <param name="dictionaries">Dictionaries to copy from</param>
        public MetaData(params IDictionary<string, string>[] dictionaries) : base(new Dictionary<string, string>())
        {
            foreach (var dic in dictionaries)
            {
                if (dic == null || dic.Count < 1) continue;
                foreach (var kvpair in dic)
                {
                    this.Dictionary[kvpair.Key] = kvpair.Value;
                }
            }
        }
    }
}