using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models
{
    /// <summary>
    /// Class used to read from the stream in a buffered manner
    /// </summary>
    public class ParametersBuffer: IDisposable
    {
        private bool isDisposed = false;
        private ILogger logger = Logging.CreateLogger(typeof(ParameterDataRaw));

        // Configuration of the buffer
        private int? bufferTimeout = null;
        private int? packetSize = null;
        private long? timeSpanInNanoseconds = null;
        private Func<ParameterDataTimestamp, bool> customTriggerBeforeEnqueue = null;
        private Func<ParameterData, bool> customTrigger = null;
        private Func<ParameterDataTimestamp, bool> filter = null;
        bool bufferingDisabled = true;

        private string[] parametersFilter; // Filtered parameters for reading and writing 
        
        private HashSet<string> parametersFilterSet; // Contains same data as the parametersFilter just in form of HashSet

        private readonly bool mergeOnFlush;
        private readonly bool cleanOnFlush;

        /// <summary>
        /// Event invoked when ParameterData is read from the buffer
        /// </summary>
        public event Action<ParameterData> OnRead;

        /// <summary>
        /// Event invoked when ParameterDataRaw is read from the buffer
        /// </summary>
        public event Action<ParameterDataRaw> OnReadRaw;

        // List representing internal data structure of the buffer
        private List<ParameterDataRaw> bufferedFrames = new List<ParameterDataRaw>();
        private object _lock;
        private int totalRowsCount = 0; // Totals rows count in bufferedFrames

        private long minTimeSpan = Int64.MaxValue;
        private long maxTimeSpan = Int64.MinValue;

        internal readonly Timer flushBufferTimeoutTimer; // Timer for Timeout buffer configuration
        
        private bool timerEnabled; // Additional timer control, because stopping timer is not behaving as expected in environments (linux/mono) we tested with

        /// <summary>
        /// Initializes a new instance of <see cref="ParametersBuffer"/>
        /// </summary>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <param name="mergeOnFlush">Merge timestamps with the same timestamp and tags when releasing data from the buffer</param>
        /// <param name="cleanOnFlush">Clean timestamps with only null values when releasing data from the buffer</param>
        internal ParametersBuffer(ParametersBufferConfiguration bufferConfiguration, string[] parametersFilter = null, bool mergeOnFlush = true, bool cleanOnFlush = true)
        {
            this.parametersFilter = parametersFilter == null ? new string[0] : parametersFilter;
            this.parametersFilterSet = new HashSet<string>(this.parametersFilter);
            this.mergeOnFlush = mergeOnFlush;
            this.cleanOnFlush = cleanOnFlush;
            this._lock = new object();

            timerEnabled = false;
            this.flushBufferTimeoutTimer = new Timer(OnFlushBufferTimeoutTimerEvent, this, Timeout.Infinite, Timeout.Infinite); // Create disabled flush time

            this.ConfigureBuffer(bufferConfiguration);
        }

        private void ConfigureBuffer(ParametersBufferConfiguration bufferConfiguration)
        {
            bufferConfiguration = bufferConfiguration ?? new ParametersBufferConfiguration();

            this.PacketSize = bufferConfiguration.PacketSize;
            this.BufferTimeout = bufferConfiguration.BufferTimeout;
            this.TimeSpanInNanoseconds = bufferConfiguration.TimeSpanInNanoseconds;
            this.CustomTrigger = bufferConfiguration.CustomTrigger;
            this.CustomTriggerBeforeEnqueue = bufferConfiguration.CustomTriggerBeforeEnqueue;
            this.Filter = bufferConfiguration.Filter;
        }

        /// <summary>
        /// Packet Size configuration. <see cref="ParametersBufferConfiguration.PacketSize"/>
        /// </summary>
        public int? PacketSize
        {
            get => this.packetSize;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }
                this.packetSize = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// Timeout configuration. <see cref="ParametersBufferConfiguration.BufferTimeout"/>
        /// </summary>
        public int? BufferTimeout
        {
            get
            {
                return this.bufferTimeout;
            }
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }

                if (this.bufferTimeout == value) return;
                this.bufferTimeout = value;
                this.UpdateIfAllConditionsAreNull();

                if (this.bufferTimeout == null)
                {
                    lock (this.flushBufferTimeoutTimer)
                    {
                        timerEnabled = false;
                        flushBufferTimeoutTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
                    }
                }
                else
                {
                    lock (this.flushBufferTimeoutTimer)
                    {
                        timerEnabled = true;
                        flushBufferTimeoutTimer.Change((int)this.bufferTimeout, Timeout.Infinite); // Reset / Enable flush timer
                    }
                }

            }
        }


        /// <summary>
        /// TimeSpan configuration in Nanoseconds. <see cref="ParametersBufferConfiguration.TimeSpanInNanoseconds"/>
        /// </summary>
        public long? TimeSpanInNanoseconds
        {
            get => this.timeSpanInNanoseconds;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }
                this.timeSpanInNanoseconds = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// TimeSpan configuration in Milliseconds. <see cref="ParametersBufferConfiguration.TimeSpanInMilliseconds"/>
        /// </summary>
        public long? TimeSpanInMilliseconds
        {
            get
            {
                return TimeSpanInNanoseconds / (long)1e6;
            }
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }

                TimeSpanInNanoseconds = value * (long)1e6;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// Filter configuration. <see cref="ParametersBufferConfiguration.Filter"/>
        /// </summary>
        public Func<ParameterDataTimestamp, bool> Filter
        {
            get => this.filter;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }
                this.filter = value;
            }
        }

        /// <summary>
        /// Gets or set the custom function which is invoked before adding the timestamp to the buffer. If returns true, <see cref="ParametersBuffer.OnRead"/> is invoked before adding the timestamp to it.
        /// Defaults to null (disabled).
        /// </summary>
        public Func<ParameterDataTimestamp, bool> CustomTriggerBeforeEnqueue
        {
            get => this.customTriggerBeforeEnqueue;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }

                this.customTriggerBeforeEnqueue = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }


        /// <summary>
        /// Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, <see cref="ParametersBuffer.OnRead"/> is invoked with the entire buffer content
        /// Defaults to null (disabled).
        /// </summary>
        public Func<ParameterData, bool> CustomTrigger
        {
            get => this.customTrigger;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(ParametersBuffer));
                }
                
                this.customTrigger = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// Writes a chunck of data into the buffer
        /// </summary>
        /// <param name="parameterDataRaw">Data in <see cref="ParametersBuffer.OnRead"/> format</param>
        protected internal void WriteChunk(Process.Models.ParameterDataRaw parameterDataRaw)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(ParametersBuffer));
            }
            this.logger.LogTrace("Writing data into the buffer. The data size is {size}", parameterDataRaw.Timestamps.Length);

            // Filter
            if (this.filter != null)
            {
                parameterDataRaw = FilterDataFrameByFilterFunction(parameterDataRaw, this.filter);
            }

            lock (this._lock)
            {

                var epoch = parameterDataRaw.Epoch;
                int startIndex = 0;
                ParameterData parameterData = null;

                for (var i = 0; i < parameterDataRaw.Timestamps.Length; i++)
                {
                    var flushCondition = EvaluateFlushDataConditionsBeforeEnqueue(parameterDataRaw, i);
                    if (!flushCondition && this.customTriggerBeforeEnqueue != null)
                    {
                        if (parameterData == null)
                        {
                            parameterData = new ParameterData(parameterDataRaw, null, false, false);
                        }

                        flushCondition = this.customTriggerBeforeEnqueue(parameterData.Timestamps[i]);
                    }

                    if (flushCondition)
                    {
                        // add pending rows before flushing
                        var splittedData = SelectPdrRows(parameterDataRaw, startIndex, i - startIndex);
                        bufferedFrames.Add(splittedData);

                        FlushData(false);

                        startIndex = i;
                    }

                    // Enqueue
                    this.totalRowsCount++;

                    if (EvaluateFlushDataConditionsAfterEnqueue(parameterDataRaw, startIndex, i))
                    {
                        var splittedData = SelectPdrRows(parameterDataRaw, startIndex, i - startIndex + 1);
                        bufferedFrames.Add(splittedData);

                        FlushData(false);

                        startIndex = i + 1;
                    }

                }

                if (startIndex < parameterDataRaw.Timestamps.Length)
                {
                    this.bufferedFrames.Add(SelectPdrRows(parameterDataRaw, startIndex));
                }
            }

            lock (_lock)
            {
                if (this.bufferedFrames.Any() && this.bufferTimeout != null)
                {
                    lock (flushBufferTimeoutTimer)
                    {
                        timerEnabled = true;
                        flushBufferTimeoutTimer.Change((int)this.bufferTimeout,
                            Timeout.Infinite); // Reset / Enable timer
                    }
                }
            }

        }

        private bool EvaluateFlushDataConditionsBeforeEnqueue(ParameterDataRaw parameterDataRaw, int timestampRawIndex)
        {
            if (this.timeSpanInNanoseconds != null)
            {
                var nano = parameterDataRaw.Timestamps[timestampRawIndex];

                this.minTimeSpan = Math.Min(this.minTimeSpan, nano);
                this.maxTimeSpan = Math.Max(this.maxTimeSpan, nano);

                var nsDiff = this.maxTimeSpan - this.minTimeSpan;
                if (nsDiff >= this.timeSpanInNanoseconds)
                {
                    // Reset min/max to this value, as will be flushing everything but this
                    this.minTimeSpan = nano;
                    this.maxTimeSpan = nano;
                    return true;
                }
            }

            return false;
        }

        private bool EvaluateFlushDataConditionsAfterEnqueue(ParameterDataRaw parameterDataRawInProgress, int startIndex, int endIndex)
        {
            if (this.bufferingDisabled)
            {
                return true;
            }

            if (this.totalRowsCount >= this.packetSize) return true; // can only evaluate to true if packetsize isn't null

            if (this.customTrigger != null)
            {
                if (this.CustomTrigger(GenerateParameterDataFromBuffer(parameterDataRawInProgress, startIndex, endIndex))) return true;
            }

            return false;
        }

        private ParameterData GenerateParameterDataFromBuffer(ParameterDataRaw parameterDataRawInProgress, int startIndex, int endIndex)
        {
            List<ParameterDataTimestamp> timestamps = new List<ParameterDataTimestamp>();
            ParameterData parameterData = null;

            // Rows already on buffer
            foreach (var raw in this.bufferedFrames)
            {
                parameterData = new ParameterData(raw, null, false, false);
                foreach (var parameterDataTimestamp in parameterData.Timestamps)
                {
                    timestamps.Add(parameterDataTimestamp);
                }
            }

            // Row on current processing parameterDataRaw
            parameterData = new ParameterData(parameterDataRawInProgress, null, false, false);
            for(var i = startIndex; i <= endIndex; i++) 
            {
                timestamps.Add(parameterData.Timestamps[i]);
            }

            return new ParameterData(timestamps, false, false);
        }
        
        /// <summary>
        /// Flush data from the buffer and release it to make it available for Read events subscribers
        /// </summary>
        /// <param name="force">If true is flushing data even when disposed</param>
        internal void FlushData(bool force)
        {
            if (!force && isDisposed)
            {
                throw new ObjectDisposedException(nameof(ParametersBuffer));
            }

            this.logger.LogTrace("Executing buffer release. Total {totalRowsCount} rows in {chunksCount} raw chunks.", this.totalRowsCount, this.bufferedFrames.Count);
            
            if (this.totalRowsCount > 0)
            {
                void RaiseData() // private function to help early return
                {
                    List<ParameterDataRaw> loadedData;

                    lock (this._lock)
                    {
                        if (this.totalRowsCount == 0) return; // check again in case it changes since entering the condition
                        loadedData = new List<ParameterDataRaw>(this.bufferedFrames);
                        if (loadedData.Count == 0) return; // this shouldn't be possible any more, but safer code won't hurt
                        this.totalRowsCount = 0;
                        this.bufferedFrames = new List<ParameterDataRaw>();
                    }

                    if (this.OnRead == null && this.OnReadRaw == null) return;
                    
                    var newPdrw = this.ConcatDataFrames(loadedData);
                    if (this.mergeOnFlush)
                    {
                        newPdrw = this.MergeTimestamps(newPdrw);
                    }

                    this.logger.LogTrace("Buffer released. After merge and clean new data contains {rows} rows.", newPdrw.Timestamps.Length);

                    if (newPdrw.Timestamps.Length <= 0) return;
                    this.OnReadRaw?.Invoke(newPdrw);

                    if (this.OnRead == null) return;
                    var data = new Streaming.Models.ParameterData(newPdrw, this.parametersFilter, false, false);
                    this.OnRead.Invoke(data);
                }

                RaiseData();
            }

            lock (flushBufferTimeoutTimer)
            {
                timerEnabled = false;
                flushBufferTimeoutTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
            }
        }

        private void UpdateIfAllConditionsAreNull()
        {
            this.bufferingDisabled = this.BufferTimeout == null &&
                   this.CustomTrigger == null &&
                   this.CustomTriggerBeforeEnqueue == null &&
                   this.PacketSize == null &&
                   this.TimeSpanInMilliseconds == null &&
                   this.TimeSpanInNanoseconds == null;
        }

        private void OnFlushBufferTimeoutTimerEvent(object state)
        {
            if (!timerEnabled) return;
            this.FlushData(false);
        }

        private ParameterDataRaw FilterDataFrameByFilterFunction(ParameterDataRaw data, Func<ParameterDataTimestamp, bool> filter)
        {
            var filteredRows = new List<int>();

            var parameterData = new Streaming.Models.ParameterData(data, null, false, false);
            
            // Indexes of elements which ran over the filter
            for (var i = 0; i < parameterData.Timestamps.Count; i++)
            {
                if (filter(parameterData.Timestamps[i]))
                {
                    filteredRows.Add(i);
                }
            }
            
            return this.SelectPdrRowsByMask(data, filteredRows);
        }

        /// <summary>
        /// Get row subset of the parameterDataRaw starting from startIndex and end at startIndex + Count
        /// </summary>
        protected ParameterDataRaw SelectPdrRows(ParameterDataRaw parameterDataRaw, int startIndex, int count)
        {
            if (startIndex == 0 && count >= parameterDataRaw.Timestamps.Length)
            {
                return parameterDataRaw;
            }
            return this.SelectPdrRowsByMask(parameterDataRaw, Enumerable.Range(startIndex, count).ToList());
        }

        /// <summary>
        /// Get row subset of the parameterDataRaw starting from startIndex until the last timestamp available
        /// </summary>
        protected ParameterDataRaw SelectPdrRows(ParameterDataRaw parameterDataRaw, int startIndex)
        {
            return SelectPdrRows(parameterDataRaw, startIndex, parameterDataRaw.Timestamps.Length - startIndex);
        }

        /// <summary>
        /// Get row subset of the parameterDataRaw from a list of selected indexes
        /// </summary>
        /// <param name="parameterDataRaw">Original data</param>
        /// <param name="filteredRows">IEnumerable containing indexes of rows to select (e.g. [0,3,5,6])</param>
        protected ParameterDataRaw SelectPdrRowsByMask(ParameterDataRaw parameterDataRaw, List<int> filteredRows){
            if(filteredRows.Count() <= 0)
                return new ParameterDataRaw(
                    parameterDataRaw.Epoch,
                    new long[0],
                    new Dictionary<string, double?[]>(),
                    new Dictionary<string, string[]>(),
                    new Dictionary<string, byte[][]>(),
                    new Dictionary<string, string[]>()
                );

            // Filter all values by the filteredRows masks
            var newNumericValues = GenerateDictionaryMaskFilterMethod(parameterDataRaw.NumericValues, filteredRows);
            var newStringValues = GenerateDictionaryMaskFilterMethod(parameterDataRaw.StringValues, filteredRows);
            var newBinaryValues = GenerateDictionaryMaskFilterMethod(parameterDataRaw.BinaryValues, filteredRows);
            var newTagsValues = GenerateDictionaryMaskFilterMethod(parameterDataRaw.TagValues, filteredRows);

            return new ParameterDataRaw(
                parameterDataRaw.Epoch,
                GenerateArrayMaskFilterMethod(parameterDataRaw.Timestamps, filteredRows),
                newNumericValues,
                newStringValues,
                newBinaryValues,
                newTagsValues
            );
        }

        /// <summary>
        /// Generic function to filter Array rows of a Dictionary of mapped columns in a efficent way
        /// </summary>
        /// <param name="originalDictionary">Original dictionary</param>
        /// <param name="selectRows">List of indexes of the array to filter</param>
        /// <returns>Filtered dictionary</returns>
        private static Dictionary<string, T[]> GenerateDictionaryMaskFilterMethod<T>(Dictionary<string, T[]> originalDictionary, List<int> selectRows)
        {
            var newDictionary = new Dictionary<string, T[]>(originalDictionary.Count);
            foreach (var kvp in originalDictionary)
            {
                newDictionary.Add(kvp.Key, GenerateArrayMaskFilterMethod(kvp.Value, selectRows));
            }

            return newDictionary;
        }

        /// <summary>
        /// Generic function to filter rows by mapping the filtered index of the original array
        /// </summary>
        /// <param name="inp">Original array</param>
        /// <param name="selectRows">List of indexes of the array to filter</param>
        /// <returns>Filtered array</returns>
        private static T[] GenerateArrayMaskFilterMethod<T>(T[] inp, List<int> selectRows)
        {
            T[] ret = new T[selectRows.Count()];
            int index = 0;
            for (var i = 0; i < selectRows.Count; i++)
            {
                ret[index++] = inp[selectRows[i]];
            }
            return ret;
        }

        /// <summary>
        /// Copy one timestamp to another index of the buffer
        /// </summary>
        /// <param name="parameterDataRaw">Buffer data</param>
        /// <param name="sourceIndex">Index of the timestamp to copy</param>
        /// <param name="targetIndex">Target index where to copy timestamp</param>
        protected void CopyParameterDataRawIndex(ParameterDataRaw parameterDataRaw, int sourceIndex, int targetIndex)
        {
            CopyIndexWithinDataframe(parameterDataRaw.NumericValues, sourceIndex, targetIndex);
            CopyIndexWithinDataframe(parameterDataRaw.BinaryValues, sourceIndex, targetIndex);
            CopyIndexWithinDataframe(parameterDataRaw.StringValues, sourceIndex, targetIndex);
            CopyIndexWithinDataframe(parameterDataRaw.TagValues, sourceIndex, targetIndex);
        }

        private static bool CopyIndexWithinDataframe<T>(Dictionary<string, T[]> dict, int sourceIndex, int targetIndex)
        {
            foreach (var keyValuePair in dict)
            {
                var arr = dict[keyValuePair.Key];
                var value = arr[sourceIndex];
                if (value != null)
                {
                    arr[targetIndex] = value;
                }
            }

            return true;
        }

        /// <summary>
        /// Merge existing timestamps with the same timestamp and tags
        /// </summary>
        /// <param name="parameterDataRaw">Data to merge</param>
        /// <returns>New object with the proper length containing merged values</returns>
        protected ParameterDataRaw MergeTimestamps(ParameterDataRaw parameterDataRaw)
        {
            Dictionary<(long, long), int> timestampsDict = new Dictionary<(long, long), int>();
            int rows = parameterDataRaw.Timestamps.Length;
            
            long[] tagsHash = Enumerable.Repeat<long>(397, rows).ToArray();
            foreach (var keyValuePair in parameterDataRaw.TagValues)
            {
                var tag = keyValuePair.Key;
                string[] tags = keyValuePair.Value;
                for (var i = 0; i < rows; i++)
                {
                    var value = tags[i];
                    if (value != null)
                    {
                        tagsHash[i] ^= value.GetHashCode();
                        tagsHash[i] ^= tag.GetHashCode();
                    }
                }
            }
            
            for (var i = 0; i < rows; i++)
            {
                long timestamp = parameterDataRaw.Timestamps[i];
                var key = (timestamp, tagsHash[i]);
                // I have already seen this timestamp/tags in the past >> copy current row to the previous row
                if (timestampsDict.TryGetValue(key, out var value))
                {
                    this.CopyParameterDataRawIndex(parameterDataRaw, i, value);
                }
                else
                {
                    timestampsDict.Add(key, i);
                }
            }

            if (timestampsDict.Count() == parameterDataRaw.Timestamps.Length)
            {
                // No filtered rows >> no need for allocating new Array
                // Since no change and no modifications were done to the original PDR
                return parameterDataRaw;
            }

            return this.SelectPdrRowsByMask(parameterDataRaw, timestampsDict.Values.ToList());
        }

        /// <summary>
        /// Concatenate list of ParameterDataRaws into a single ParameterDataRaw
        /// </summary>
        /// <param name="parameterDataRaws">List of data to concatenate</param>
        /// <returns>New object with the proper length containing concatenated data</returns>
        protected ParameterDataRaw ConcatDataFrames(List<ParameterDataRaw> parameterDataRaws)
        {
            if (parameterDataRaws.Count == 0) return new ParameterDataRaw();
            long newEpoch = parameterDataRaws.First().Epoch; 
            
            // Timestamps must be shifted if the epoch is different than the target epoch
            long[] newTimestamps = ArrayConcatMethod(
                parameterDataRaws.Select(e =>
                {
                    if(e.Epoch == newEpoch)
                        return e.Timestamps;
                    long epochDiff = e.Epoch - newEpoch;
                    //TODO: can allocate twice ( potential performance improvement )
                    return e.Timestamps.Select((ts) => ts + epochDiff).ToArray();
                }).ToArray());

            int rowsLen = newTimestamps.Length;

            Dictionary<string, string[]> newTagValues = new Dictionary<string, string[]>();
            Dictionary<string, double?[]> newNumericValues = new Dictionary<string, double?[]>();
            Dictionary<string, string[]> newStringValues = new Dictionary<string, string[]>();
            Dictionary<string, byte[][]> newBinaryValues = new Dictionary<string, byte[][]>();

            int index = 0;
            if (parametersFilter.Length > 0)
            {
                for (var i = 0; i < parameterDataRaws.Count(); i++)
                {
                    ExtendDictionaryWithKeyFilter(parameterDataRaws[i].NumericValues, this.parametersFilterSet, index, rowsLen, newNumericValues);
                    ExtendDictionaryWithKeyFilter(parameterDataRaws[i].BinaryValues, this.parametersFilterSet, index, rowsLen, newBinaryValues);
                    ExtendDictionaryWithKeyFilter(parameterDataRaws[i].StringValues, this.parametersFilterSet, index, rowsLen, newStringValues);
                    ExtendDictionary(parameterDataRaws[i].TagValues, index, rowsLen, newTagValues);
                    index += parameterDataRaws[i].Timestamps.Length;
                }
            }
            else
            {
                for (var i = 0; i < parameterDataRaws.Count(); i++)
                {
                    ExtendDictionary(parameterDataRaws[i].NumericValues, index, rowsLen, newNumericValues);
                    ExtendDictionary(parameterDataRaws[i].BinaryValues, index, rowsLen, newBinaryValues);
                    ExtendDictionary(parameterDataRaws[i].StringValues, index, rowsLen, newStringValues);
                    ExtendDictionary(parameterDataRaws[i].TagValues, index, rowsLen, newTagValues);
                    index += parameterDataRaws[i].Timestamps.Length;
                }
            }

            var ret = new ParameterDataRaw(
                newEpoch,
                newTimestamps,
                newNumericValues,
                newStringValues,
                newBinaryValues,
                newTagValues
            );

            if (parametersFilter.Length > 0 || this.cleanOnFlush)
            {
                return FilterOutNullRows(ret);
            }
            else
            {
                return ret;
            }
        }

        /// <summary>
        /// Function which concatenate arrays in an efficient way
        /// </summary>
        /// <param name="inp">List of arrays to concatenate</param>
        /// <returns>Concatenated array</returns>
        private static T[] ArrayConcatMethod<T>(IEnumerable<T[]> inp)
        {
            var newRowCount = inp.Select((arg => arg.Length)).Aggregate(((i, i1) => i + i1));
            T[] ret = new T[newRowCount];
            int index = 0;
            foreach (var item in inp)
            {
                item.CopyTo(ret, index);
                index += item.Length;
            }
            return ret;
        }

        private static void ExtendDictionary<T>(Dictionary<string, T[]> mergeWith, int index, int defaultLen, Dictionary<string, T[]> originalDict)
        {
            foreach (var keyValuePair in mergeWith)
            {
                bool isNotNull = false;
                for (var i = 0; i < keyValuePair.Value.Length; i++)
                {
                    if (keyValuePair.Value[i] != null)
                    {
                        isNotNull = true;
                        break;
                    }
                }

                // Execute the copying logic only when the input array contains any non-null value
                if (isNotNull)
                {
                    if (!originalDict.TryGetValue(keyValuePair.Key, out var values))
                    {
                        values = new T[defaultLen];
                        originalDict.Add(keyValuePair.Key, values);
                    }

                    keyValuePair.Value.CopyTo(values, index);
                }
            }
        }

        private static void ExtendDictionaryWithKeyFilter<T>(Dictionary<string, T[]> mergeWith, HashSet<string> keyFilter, int index, int defaultLen, Dictionary<string, T[]> originalDict)
        {
            foreach (var keyValuePair in mergeWith)
            {
                if (keyFilter.Contains(keyValuePair.Key))
                {
                    bool isNotNull = false;
                    for (var i = 0; i < keyValuePair.Value.Length; i++)
                    {
                        if (keyValuePair.Value[i] != null)
                        {
                            isNotNull = true;
                            break;
                        }
                    }

                    // Execute the copying logic only when the input array contains any non-null value
                    if (isNotNull)
                    {
                        if (!originalDict.TryGetValue(keyValuePair.Key, out var values))
                        {
                            values = new T[defaultLen];
                            originalDict.Add(keyValuePair.Key, values);
                        }

                        keyValuePair.Value.CopyTo(values, index);
                    }
                }
            }
        }

        /// <summary>
        /// Remove rows that only contain null values
        /// </summary>
        /// <param name="parameterDataRaw">Data to be cleaned</param>
        /// <returns>Cleaned data without the rows containing only null values</returns>
        protected ParameterDataRaw FilterOutNullRows(ParameterDataRaw parameterDataRaw)
        {
            // Contains only 0 and 1 values, but the datatype is in the form of byte so we can perform the xor operation under this array
            byte[] filter = Enumerable.Repeat<byte>(0, parameterDataRaw.Timestamps.Length).ToArray();

            // Count of the non-null rows
            int count = 0;

            void applyFilterRows<T>(Dictionary<string, T[]> inp)
            {
                foreach (var keyValuePair in inp)
                {
                    for (var i = 0; i < keyValuePair.Value.Length; i++)
                    {
                        if (keyValuePair.Value[i] != null)
                        {
                            // Returns 1 only if the filter is 0 so then it would increment the cnt variable
                            count += filter[i] ^ 1;
                            filter[i] = 1;
                        }
                    }
                }
            }

            applyFilterRows(parameterDataRaw.NumericValues);
            applyFilterRows(parameterDataRaw.BinaryValues);
            applyFilterRows(parameterDataRaw.StringValues);

            if (count >= parameterDataRaw.Timestamps.Length)
            {
                // No rows to filter
                return parameterDataRaw;
            }
            
            T[] AllocForFilter<T>(T[] inpArr)
            {
                var outArr = new T[count];
                var outIndex = 0;
                for (var i = 0; i < inpArr.Length; i++)
                {
                    if (filter[i]>0)
                    {
                        outArr[outIndex++] = inpArr[i];
                    }
                }
                return outArr;
            }

            Dictionary<string, T[]> GenerateFilteredDictionary<T>(Dictionary<string, T[]> originalDictionary)
            {
                var newDictionary = new Dictionary<string, T[]>(originalDictionary.Count);
                foreach (var kvp in originalDictionary)
                {
                    newDictionary.Add(kvp.Key, AllocForFilter(kvp.Value));
                }

                return newDictionary;
            }


            return new ParameterDataRaw(
                parameterDataRaw.Epoch,
                AllocForFilter(parameterDataRaw.Timestamps),
                GenerateFilteredDictionary(parameterDataRaw.NumericValues),
                GenerateFilteredDictionary(parameterDataRaw.StringValues),
                GenerateFilteredDictionary(parameterDataRaw.BinaryValues),
                GenerateFilteredDictionary(parameterDataRaw.TagValues)
            );
        }

        /// <summary>
        /// Dispose the buffer. It releases data out before the actual disposal.
        /// </summary>
        public virtual void Dispose()
        {
            this.logger.LogTrace("Disposing buffer.");
            if (this.isDisposed) return;
            this.isDisposed = true;
            this.FlushData(true);
            flushBufferTimeoutTimer?.Dispose();
        }


    }
}
