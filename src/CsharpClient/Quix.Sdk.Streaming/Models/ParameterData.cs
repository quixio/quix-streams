using Quix.Sdk.Process.Models.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Streaming.Utils;

namespace Quix.Sdk.Streaming.Models
{
    /// <summary>
    /// Represents a collection of <see cref="ParameterDataTimestamp"/>
    /// </summary>
    public class ParameterData
    {
        internal Process.Models.ParameterDataRaw rawData;
        internal Dictionary<string, Parameter> parameterList;
        internal List<int> timestampsList;

        private int nextIndexRawData = 0;
        private bool removedTimestamps = false;

        internal bool[] epochsIncluded;

        /// <summary>
        /// Create a new empty Parameter Data instance to allow create new timestamps and parameters values from scratch
        /// </summary>
        /// <param name="capacity">The number of timestamps that the new Parameter Data initially store. 
        /// Using this parameter when you know the number of Timestamps you need to store will increase the performance of the writing.</param>
        public ParameterData(int capacity = 10)
        {
            this.rawData = this.EmptyRawData(capacity);
            this.epochsIncluded = new bool[capacity];
            this.timestampsList = new List<int>();
            this.parameterList = new Dictionary<string, Parameter>();
        }

        /// <summary>
        /// Creates a new instance of <see cref="ParameterData"/> based on a <see cref="ParameterDataRaw"/> instance
        /// </summary>
        /// <param name="rawData">Parameter Data Raw instance from where lookup the data</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <param name="merge">Merge duplicated timestamps</param>
        /// <param name="clean">Clean timestamps without values</param>
        public ParameterData(Process.Models.ParameterDataRaw rawData, string[] parametersFilter = null, bool merge = true, bool clean = true)
        {
            this.rawData = rawData;
            this.epochsIncluded = new bool[rawData.Timestamps.Count()];
            this.timestampsList = Enumerable.Range(0, rawData.Timestamps.Count()).ToList();

            this.parameterList = GetParameterList();
            this.ApplyParametersFilter(parametersFilter);

            if (merge)
            {
                // Merge duplicate timestamps
                this.MergeDuplicateTimestamps();
            }

            if (clean)
            {
                // Clean empty Parameter Values and Empty timestamps
                this.CleanEmptyTimestampsAndValues();
            }
        }

        /// <summary>
        /// Clone the Parameter Data
        /// </summary>
        /// <param name="parametersFilter">List of parameters to filter when we clone the data.</param>
        /// <returns>Cloned data</returns>
        public ParameterData Clone(params string[] parametersFilter)
        {
            var data = new ParameterData();
            data.CloneFrom(this, parametersFilter);
            return data;
        }

        private void CloneFrom(Streaming.Models.ParameterData data, string[] parametersFilter = null)
        {
            this.rawData = data.rawData.Clone();
            this.epochsIncluded = (bool[])data.epochsIncluded.Clone();
            this.timestampsList = data.timestampsList.ToList();
            this.nextIndexRawData = data.nextIndexRawData;
            this.removedTimestamps = data.removedTimestamps;

            this.parameterList = GetParameterList();
            this.ApplyParametersFilter(parametersFilter);

            // Clean empty Parameter values and empty Timestamps
            this.CleanEmptyTimestampsAndValues();
        }

        /// <summary>
        /// Creates a new instance of <see cref="ParameterData"/> with the provided timestamps
        /// </summary>
        /// <param name="timestamps">The timestamps with parameter data</param>
        /// <param name="merge">Merge duplicated timestamps</param>
        /// <param name="clean">Clean timestamps without values</param>
        public ParameterData(List<ParameterDataTimestamp> timestamps, bool merge = true, bool clean = true)
        {
            this.rawData = this.EmptyRawData(timestamps.Count);
            this.epochsIncluded = new bool[timestamps.Count];
            this.timestampsList = new List<int>();
            this.parameterList = new Dictionary<string, Parameter>();

            AddTimestamps(timestamps);

            if (merge)
            {
                // Merge duplicate timestamps
                this.MergeDuplicateTimestamps();
            }

            if (clean)
            {
                // Clean empty Parameter Values and Empty timestamps
                this.CleanEmptyTimestampsAndValues();
            }
        }

        private Dictionary<string, Parameter> GetParameterList()
        {
            var list = new Dictionary<string, Parameter>();

            foreach (var kv in this.rawData.NumericValues)
            {
                list.Add(kv.Key, new Parameter(kv.Key, kv.Value));
            }

            foreach (var kv in this.rawData.StringValues)
            {
                list.Add(kv.Key, new Parameter(kv.Key, kv.Value));
            }

            foreach (var kv in this.rawData.BinaryValues)
            {
                list.Add(kv.Key, new Parameter(kv.Key, kv.Value));
            }

            return list;
        }

        private void ApplyParametersFilter(string[] parametersFilter)
        {
            parametersFilter = parametersFilter?.Length > 0 ? parametersFilter : null;
            if (parametersFilter == null)
            {
                return;
            }

            foreach (var parameter in this.parameterList.Values)
            {
                if (parametersFilter.Contains(parameter.ParameterId))
                {
                    continue;
                }

                this.rawData.NumericValues.Remove(parameter.ParameterId);
                this.rawData.StringValues.Remove(parameter.ParameterId);
                this.rawData.BinaryValues.Remove(parameter.ParameterId);

                this.parameterList.Remove(parameter.ParameterId);
            }
        }

        private void AddTimestamps(List<ParameterDataTimestamp> timestamps)
        {
            var sizeNeeded = this.timestampsList.Count() + timestamps.Count;

            this.CheckRawDataSize(sizeNeeded);

            this.timestampsList.AddRange(Enumerable.Range(this.nextIndexRawData, timestamps.Count).ToList());

            for (var i = 0; i < timestamps.Count; i++)
            {
                var timestamp = timestamps[i];
                var newTimestamp = this.Timestamps[this.nextIndexRawData];

                newTimestamp.TimestampNanoseconds = timestamp.TimestampNanoseconds;
                newTimestamp.EpochIncluded = timestamp.EpochIncluded;

                foreach (var parameter in timestamp.Parameters.Values)
                {
                    newTimestamp.AddValue(parameter.ParameterId, parameter);
                }

                newTimestamp.AddTags(timestamp.Tags);

                this.nextIndexRawData++;
            }
        }

        private ParameterDataRaw EmptyRawData(int size)
        {
            return new ParameterDataRaw()
            {
                Epoch = 0,
                Timestamps = new long[size],
                NumericValues = new Dictionary<string, double?[]>(),
                StringValues = new Dictionary<string, string[]>(),
                BinaryValues = new Dictionary<string, byte[][]>(),
                TagValues =  new Dictionary<string, string[]>()
            };
        }

        /// <summary>
        /// Resize the collection of timestamps according to the needs
        /// </summary>
        /// <param name="sizeNeeded">Size needed</param>
        private void CheckRawDataSize(int sizeNeeded)
        {
            if (sizeNeeded <= this.rawData.Timestamps.Length)
            {
                return;
            }

            // 5 => 10
            // 25 => 100
            // 100 => 100
            // 101 => 1000
            // 999 => 1000
            // 1000 => 1000
            // 1001 => 10000
            var newSize = (int)(Math.Pow(10, Math.Ceiling(Math.Log10(sizeNeeded))));

            this.ResizeRawData(newSize);
            this.parameterList = GetParameterList();
        }


        private void ResizeRawData(int newSize)
        {
            Array.Resize(ref this.rawData.Timestamps, newSize);
            Array.Resize(ref this.epochsIncluded, newSize);
            foreach (var kv in this.rawData.NumericValues.ToList())
            {
                if (this.rawData.NumericValues.TryGetValue(kv.Key, out var values))
                {
                    Array.Resize(ref values, newSize);
                    this.rawData.NumericValues[kv.Key] = values;
                }
            }
            foreach (var kv in this.rawData.StringValues.ToList())
            {
                if (this.rawData.StringValues.TryGetValue(kv.Key, out var values))
                {
                    Array.Resize(ref values, newSize);
                    this.rawData.StringValues[kv.Key] = values;
                }
            }
            foreach (var kv in this.rawData.BinaryValues.ToList())
            {
                if (this.rawData.BinaryValues.TryGetValue(kv.Key, out var values))
                {
                    Array.Resize(ref values, newSize);
                    this.rawData.BinaryValues[kv.Key] = values;
                }
            }
            foreach (var kv in this.rawData.TagValues.ToList())
            {
                if (this.rawData.TagValues.TryGetValue(kv.Key, out var values))
                {
                    Array.Resize(ref values, newSize);
                    this.rawData.TagValues[kv.Key] = values;
                }
            }
        }

        internal Process.Models.ParameterDataRaw ConvertToProcessData(bool merge = true, bool clean = true)
        {
            if (merge)
            {
                this.MergeDuplicateTimestamps();
            }

            if (clean)
            {
                this.CleanEmptyTimestampsAndValues();
            }

            if (this.removedTimestamps || this.timestampsList.Count < this.rawData.Timestamps.Length)
            {
                return this.GenerateCleanRawData();
            }

            return this.rawData;
        }

        private ParameterDataRaw GenerateCleanRawData()
        {
            var newSize = this.timestampsList.Count();

            var newRawData = new ParameterDataRaw()
            {
                Epoch = this.rawData.Epoch,
                Timestamps = new long[newSize],
                NumericValues = rawData.NumericValues.ToDictionary(kv => kv.Key, kv => new double?[newSize]),
                StringValues = rawData.StringValues.ToDictionary(kv => kv.Key, kv => new string[newSize]),
                BinaryValues = rawData.BinaryValues.ToDictionary(kv => kv.Key, kv => new byte[newSize][]),
                TagValues = rawData.TagValues.ToDictionary(kv => kv.Key, kv => new string[newSize]),
            };

            for (var i = 0; i < this.timestampsList.Count(); i++)
            {
                var timestampRawIndex = this.timestampsList[i];

                newRawData.Timestamps[i] = this.rawData.Timestamps[timestampRawIndex];

                foreach (var kv in newRawData.NumericValues)
                {
                    kv.Value[i] = this.rawData.NumericValues[kv.Key][timestampRawIndex];
                }
                foreach (var kv in newRawData.StringValues)
                {
                    kv.Value[i] = this.rawData.StringValues[kv.Key][timestampRawIndex];
                }
                foreach (var kv in newRawData.BinaryValues)
                {
                    kv.Value[i] = this.rawData.BinaryValues[kv.Key][timestampRawIndex];
                }
                foreach (var kv in newRawData.TagValues)
                {
                    kv.Value[i] = this.rawData.TagValues[kv.Key][timestampRawIndex];
                }
            }

            return newRawData;
        }

        private void MergeDuplicateTimestamps()
        {
            var encountered = new HashSet<long>();
            var dupes = new Dictionary<long, List<int>>();
            for (var index = 0; index < this.Timestamps.Count; index++)
            {
                var parameterDataTimestamp = this.Timestamps[index];
                if (!encountered.Add(parameterDataTimestamp.TimestampNanoseconds))
                {
                    if (!dupes.TryGetValue(parameterDataTimestamp.TimestampNanoseconds, out var dupeList))
                    {
                        dupeList = new List<int>();
                        dupes[parameterDataTimestamp.TimestampNanoseconds] = dupeList;
                        // find previous dupe as we don't track it yet
                        for (var innerIndex = 0; innerIndex < index; innerIndex++)
                        {
                            var innerPdts = this.Timestamps[innerIndex];
                            if (innerPdts.TimestampNanoseconds == parameterDataTimestamp.TimestampNanoseconds)
                            {
                                dupeList.Add(innerIndex);
                                break; // found the previous dupe
                            }
                        }
                    }
                    dupeList.Add(index);
                }
            }
            
            if (dupes.Count == 0) return;

            var uniqueTimestamps = new Dictionary<(long, long), int>();            
            
            foreach (var timestamp in dupes)
            {
                foreach (var index in timestamp.Value)
                {
                    var parameterDataTimestamp = this.Timestamps[index];

                    var key = (timestamp.Key, TagsHash(parameterDataTimestamp.Tags));
                    if (!uniqueTimestamps.TryGetValue(key, out var uniqueTimestampIndex))
                    {
                        uniqueTimestamps[key] = index; // new row with Timestamp
                    }
                    else
                    {
                        if (index != uniqueTimestampIndex) // If trying to merge to timestamps that are pointing to the same instance
                        {
                            foreach (var parameter in parameterDataTimestamp.Parameters.Values)
                            {
                                if (parameter.Value != null)
                                {
                                    this.Timestamps[uniqueTimestampIndex].AddValue(parameter.ParameterId, parameter);
                                }
                            }
                        }
                    }
                }
            }

            // Remove the duplicates except the ones merged
            var timestampsToRemove = dupes.Values.SelectMany(y => y).Distinct().Except(uniqueTimestamps.Values).ToList();
            for (var i = 0; i < timestampsToRemove.Count; i++)
            {
                this.RemoveTimestamp(timestampsToRemove[i] - i);
            }
        }

        private static long TagsHash(ParameterDataTimestampTags tags)
        {
            if (tags.Count == 0) return 0;
            unchecked
            {
                var hash = 397;
                foreach (var kpair in tags)
                {
                    hash ^= kpair.Value?.GetHashCode() ?? 0;
                    hash ^= kpair.Key.GetHashCode();
                }

                return hash;
            }
        }

        private void CleanEmptyTimestampsAndValues()
        {
            var timestampsToRemove = new List<int>();

            for (var index = 0; index < this.Timestamps.Count; index++)
            {
                var parameterDataTimestamp = this.Timestamps[index];
                var empty = true;
                foreach (var parameterValue in parameterDataTimestamp.Parameters.Values)
                {
                    if (parameterValue.Value != null)
                    {
                        empty = false;
                        continue;
                    }
                }

                if (empty)
                {
                    timestampsToRemove.Add(index);
                }
            }

            for (var i = 0; i < timestampsToRemove.Count(); i++)
            {
                this.RemoveTimestamp(timestampsToRemove[i] - i);
            }
        }

        /// <summary>
        /// Gets the data as rows of <see cref="ParameterDataTimestamp"/>
        /// </summary>
        public ParameterDataTimestamps Timestamps => new ParameterDataTimestamps(this);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="dateTime">The datetime to use for adding new parameter values</param>
        /// <returns>Parameter data to add parameter values at the provided time</returns>
        public ParameterDataTimestamp AddTimestamp(DateTime dateTime) => this.AddTimestampNanoseconds(dateTime.ToUnixNanoseconds(), true);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeSpan">The time since the <see name="epochOffset"/> to add the parameter values at</param>
        /// <returns>Parameter data to add parameter values at the provided time</returns>
        public ParameterDataTimestamp AddTimestamp(TimeSpan timeSpan) => this.AddTimestampNanoseconds(timeSpan.ToNanoseconds(), false);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeMilliseconds">The time in milliseconds since the <see name="epochOffset"/> to add the parameter values at</param>
        /// <returns>Parameter data to add parameter values at the provided time</returns>
        public ParameterDataTimestamp AddTimestampMilliseconds(long timeMilliseconds) => this.AddTimestampNanoseconds(timeMilliseconds * (long)1e6, false);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeNanoseconds">The time in nanoseconds since the  <see name="epochOffset"/> to add the parameter values at</param>
        /// <returns>Parameter data to add parameter values at the provided time</returns>
        public ParameterDataTimestamp AddTimestampNanoseconds(long timeNanoseconds) => this.AddTimestampNanoseconds(timeNanoseconds, false);

        /// <summary>
        /// Starts adding a new set of parameter values at the given timestamp.
        /// </summary>
        /// <param name="timeNanoseconds">The time in nanoseconds since the  <see name="epoch"/> to add the parameter values at</param>
        /// <param name="epochIncluded">Epoch offset is included in the timestamp</param>
        /// <returns>Parameter data to add parameter values at the provided time</returns>
        internal ParameterDataTimestamp AddTimestampNanoseconds(long timeNanoseconds, bool epochIncluded)
        {
            var sizeNeeded = this.timestampsList.Count() + 1;
            this.CheckRawDataSize(sizeNeeded);

            this.timestampsList.Add(this.nextIndexRawData);

            this.rawData.Timestamps[this.nextIndexRawData] = timeNanoseconds;
            this.epochsIncluded[this.nextIndexRawData] = epochIncluded;

            var newTimestamp = new ParameterDataTimestamp(this, this.nextIndexRawData);
            this.nextIndexRawData++;

            return newTimestamp;
        }

        internal void RemoveTimestamp(int index)
        {
            this.timestampsList.RemoveAt(index);
            this.removedTimestamps = true;
        }

        /// <inheritdoc/>
        public override bool Equals(Object obj)
        {
            void MergeEpoch(ParameterDataRaw rawData)
            {
                if (rawData.Epoch == 0) return;

                for (var i = 0; i < rawData.Timestamps.Length; i++)
                {
                    rawData.Timestamps[i] += rawData.Epoch;
                }

                rawData.Epoch = 0;
            }


            if (!(obj is ParameterData c))
            {
                return false;
            }

            var rawA = this.ConvertToProcessData();
            var rawB = c.ConvertToProcessData();

            MergeEpoch(rawA);
            MergeEpoch(rawB);

            c.ConvertToProcessData().ToJson();

            return rawA.ToJson() == rawB.ToJson();
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 397;
                hash ^= this.rawData.ToJson().GetHashCode();
                //hash ^= this.rawData.GetHashCode();

                return hash;
            }
        }
    }

    /// <summary>
    /// Describes the type of a Value of a specific Timestamp / Parameter
    /// </summary>
    public enum ParameterValueType
    {
        /// <summary>
        /// The value is empty and type cannot be determined
        /// </summary>
        Empty = 0,

        /// <summary>
        /// Numeric value
        /// </summary>
        Numeric = 1,

        /// <summary>
        /// String value
        /// </summary>
        String = 2,

        /// <summary>
        /// Binary value
        /// </summary>
        Binary = 3
    }

    internal class Parameter
    {
        public Parameter(string parameterId)
        {
            this.ParameterId = parameterId;
        }

        public Parameter(string parameterId, double?[] numericValues)
        {
            this.ParameterId = parameterId;
            this.NumericValues = numericValues;
        }

        public Parameter(string parameterId, string[] stringValues)
        {
            this.ParameterId = parameterId;
            this.StringValues = stringValues;
        }

        public Parameter(string parameterId, byte[][] binaryValues)
        {
            this.ParameterId = parameterId;
            this.BinaryValues = binaryValues;
        }

        public readonly string ParameterId;

        public readonly double?[] NumericValues;

        public readonly string[] StringValues;

        public readonly byte[][] BinaryValues;

        public override bool Equals(Object obj)
        {
            if (obj == null) return false;

            return this == obj;
        }

        public override int GetHashCode()
        {
            return this.ParameterId.GetHashCode();
        }


    }
}