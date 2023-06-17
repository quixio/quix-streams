using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams.Streaming.Models.StreamConsumer;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models
{
    /// <summary>
    /// Represents a class used to consume and produce stream messages in a buffered manner.
    /// </summary>
    public class TimeseriesBuffer: IDisposable
    {
        private bool isDisposed = false;
        private ILogger logger = Logging.CreateLogger(typeof(TimeseriesDataRaw));

        // Configuration of the buffer
        private int? bufferTimeout = null;
        private int? packetSize = null;
        private long? timeSpanInNanoseconds = null;
        private long? leadingEdgeDelayInNanoseconds = null;
        private Func<TimeseriesDataTimestamp, bool> customTriggerBeforeEnqueue = null;
        private Func<TimeseriesData, bool> customTrigger = null;
        private Func<TimeseriesDataTimestamp, bool> filter = null;
        bool bufferingDisabled = true;

        private string[] parametersFilter; // Filtered parameters for consuming and producing 
        
        private HashSet<string> parametersFilterSet; // Contains same data as the parametersFilter just in form of HashSet

        private readonly bool mergeOnFlush;
        private readonly bool cleanOnFlush;

        /// <summary>
        /// Event invoked when TimeseriesData is received from the buffer
        /// </summary>
        public event EventHandler<TimeseriesDataReadEventArgs> OnDataReleased;
        
        /// <summary>
        /// Items arriving after LeadingEdgeDelay are discarded from the output topic but released in this event for further processing or forwarding.
        /// </summary>
        public event EventHandler<TimeseriesDataReadEventArgs> OnBackfill;

        /// <summary>
        /// Event invoked when TimeseriesDataRaw is received from the buffer
        /// </summary>
        public event EventHandler<TimeseriesDataRawReadEventArgs> OnRawReleased;

        // List representing internal data structure of the buffer
        private List<TimeseriesDataRaw> bufferedFrames = new List<TimeseriesDataRaw>();
        private TimeseriesDataRaw bufferedFramesDelayed = new TimeseriesDataRaw();
        private object _lock;
        private int totalRowsCount = 0; // Totals rows count in bufferedFrames

        private long minTimeSpan = Int64.MaxValue;
        private long maxTimeSpan = Int64.MinValue;
        private long leadingEdge = Int64.MinValue;
        private long lastTimestampReleased = Int64.MinValue;

        private readonly Timer flushBufferTimeoutTimer; // Timer for Timeout buffer configuration
        
        private bool timerEnabled; // Additional timer control, because stopping timer is not behaving as expected in environments (linux/mono) we tested with

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesBuffer"/>
        /// </summary>
        /// <param name="bufferConfiguration">Configuration of the buffer</param>
        /// <param name="parametersFilter">List of parameters to filter</param>
        /// <param name="mergeOnFlush">Merge timestamps with the same timestamp and tags when releasing data from the buffer</param>
        /// <param name="cleanOnFlush">Clean timestamps with only null values when releasing data from the buffer</param>
        internal TimeseriesBuffer(TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter = null, bool mergeOnFlush = true, bool cleanOnFlush = true)
        {
            this.parametersFilter = parametersFilter ?? Array.Empty<string>();
            this.parametersFilterSet = new HashSet<string>(this.parametersFilter);
            this.mergeOnFlush = mergeOnFlush;
            this.cleanOnFlush = cleanOnFlush;
            this._lock = new object();

            timerEnabled = false;
            this.flushBufferTimeoutTimer = new Timer(OnFlushBufferTimeoutTimerEvent, this, Timeout.Infinite, Timeout.Infinite); // Create disabled flush time

            this.ConfigureBuffer(bufferConfiguration);
        }

        private void ConfigureBuffer(TimeseriesBufferConfiguration bufferConfiguration)
        {
            bufferConfiguration = bufferConfiguration ?? new TimeseriesBufferConfiguration();

            this.PacketSize = bufferConfiguration.PacketSize;
            this.BufferTimeout = bufferConfiguration.BufferTimeout;
            this.TimeSpanInNanoseconds = bufferConfiguration.TimeSpanInNanoseconds;
            this.LeadingEdgeDelay = bufferConfiguration.LeadingEdgeDelay;
            this.CustomTrigger = bufferConfiguration.CustomTrigger;
            this.CustomTriggerBeforeEnqueue = bufferConfiguration.CustomTriggerBeforeEnqueue;
            this.Filter = bufferConfiguration.Filter;
        }

        /// <summary>
        /// Packet Size configuration. <see cref="TimeseriesBufferConfiguration.PacketSize"/>
        /// </summary>
        public int? PacketSize
        {
            get => this.packetSize;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }
                this.packetSize = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// Timeout configuration. <see cref="TimeseriesBufferConfiguration.BufferTimeout"/>
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
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
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
        /// TimeSpan configuration in Nanoseconds. <see cref="TimeseriesBufferConfiguration.TimeSpanInNanoseconds"/>
        /// </summary>
        public long? TimeSpanInNanoseconds
        {
            get => this.timeSpanInNanoseconds;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }
                this.timeSpanInNanoseconds = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// TimeSpan configuration in Milliseconds. <see cref="TimeseriesBufferConfiguration.TimeSpanInMilliseconds"/>
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
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }

                TimeSpanInNanoseconds = value * (long)1e6;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// Leading edge delay configuration in Milliseconds . <see cref="TimeseriesBufferConfiguration.LeadingEdgeDelay"/>
        /// </summary>
        public long? LeadingEdgeDelay
        {
            get => this.leadingEdgeDelayInNanoseconds / (long)1e6;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }
                this.leadingEdgeDelayInNanoseconds = value * (long)1e6;
                this.UpdateIfAllConditionsAreNull();
            }
        }
        
        /// <summary>
        /// Filter configuration. <see cref="TimeseriesBufferConfiguration.Filter"/>
        /// </summary>
        public Func<TimeseriesDataTimestamp, bool> Filter
        {
            get => this.filter;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }
                this.filter = value;
            }
        }

        /// <summary>
        /// Gets or set the custom function which is invoked before adding the timestamp to the buffer. If returns true, <see cref="OnDataReleased"/> is invoked before adding the timestamp to it.
        /// Defaults to null (disabled).
        /// </summary>
        public Func<TimeseriesDataTimestamp, bool> CustomTriggerBeforeEnqueue
        {
            get => this.customTriggerBeforeEnqueue;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }

                this.customTriggerBeforeEnqueue = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }


        /// <summary>
        /// Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, <see cref="OnDataReleased"/> is invoked with the entire buffer content
        /// Defaults to null (disabled).
        /// </summary>
        public Func<TimeseriesData, bool> CustomTrigger
        {
            get => this.customTrigger;
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(TimeseriesBuffer));
                }
                
                this.customTrigger = value;
                this.UpdateIfAllConditionsAreNull();
            }
        }

        /// <summary>
        /// Writes a chunk of data into the buffer
        /// </summary>
        /// <param name="timeseriesDataRaw">Data in <see cref="OnDataReleased"/> format</param>
        protected internal void WriteChunk(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(TimeseriesBuffer));
            }
            this.logger.LogTrace("Writing data into the buffer. The data size is {size}", timeseriesDataRaw.Timestamps.Length);

            // Filter
            if (this.filter != null)
            {
                timeseriesDataRaw = FilterDataFrameByFilterFunction(timeseriesDataRaw, this.filter);
            }

            if (this.leadingEdgeDelayInNanoseconds != null)
            {
                UpdateLeadingEdge(timeseriesDataRaw);
                timeseriesDataRaw = FilterAndBackfillLateArrivingData(timeseriesDataRaw);
                timeseriesDataRaw = FilterDataFrameByLeadingEdgeDelay(timeseriesDataRaw);
            }

            lock (this._lock)
            {
                if (this.bufferingDisabled)
                {
                    bufferedFrames.Add(timeseriesDataRaw);
                    this.totalRowsCount += timeseriesDataRaw.Timestamps.Length;
                    FlushData(false);
                }
                else
                {
                    var epoch = timeseriesDataRaw.Epoch;
                    int startIndex = 0;
                    TimeseriesData timeseriesData = null;

                    for (var i = 0; i < timeseriesDataRaw.Timestamps.Length; i++)
                    {
                        // Check if flush condition is met by Timespan config
                        var flushCondition = EvaluateFlushDataConditionsBeforeEnqueue(timeseriesDataRaw, i);
                        
                        // If not, check if flush condition is met by custom trigger
                        if (!flushCondition && this.customTriggerBeforeEnqueue != null)
                        {
                            if (timeseriesData == null)
                            {
                                timeseriesData = new TimeseriesData(timeseriesDataRaw, null, false, false);
                            }

                            flushCondition = this.customTriggerBeforeEnqueue(timeseriesData.Timestamps[i]);
                        }

                        // Flush if condition is met
                        if (flushCondition)
                        {
                            // add pending rows before flushing
                            var splitData = SelectPdrRows(timeseriesDataRaw, startIndex, i - startIndex);
                            bufferedFrames.Add(splitData);

                            FlushData(false);

                            startIndex = i;
                        }

                        // Enqueue, add rows to the buffer
                        this.totalRowsCount++;

                        if (EvaluateFlushDataConditionsAfterEnqueue(timeseriesDataRaw, startIndex, i))
                        {
                            var splitData = SelectPdrRows(timeseriesDataRaw, startIndex, i - startIndex + 1);
                            bufferedFrames.Add(splitData);

                            FlushData(false);

                            startIndex = i + 1;
                        }

                    }

                    if (startIndex < timeseriesDataRaw.Timestamps.Length)
                    {
                        this.bufferedFrames.Add(SelectPdrRows(timeseriesDataRaw, startIndex));
                    }
                }

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

        private bool EvaluateFlushDataConditionsBeforeEnqueue(TimeseriesDataRaw timeseriesDataRaw, int timestampRawIndex)
        {
            if (this.timeSpanInNanoseconds != null)
            {
                var nano = timeseriesDataRaw.Timestamps[timestampRawIndex];

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

        private bool EvaluateFlushDataConditionsAfterEnqueue(TimeseriesDataRaw timeseriesDataRawInProgress, int startIndex, int endIndex)
        {
            if (this.totalRowsCount >= this.packetSize) return true; // can only evaluate to true if packetsize isn't null

            if (this.customTrigger != null)
            {
                if (this.CustomTrigger(GenerateTimeseriesDataFromBuffer(timeseriesDataRawInProgress, startIndex, endIndex))) return true;
            }

            return false;
        }

        private TimeseriesData GenerateTimeseriesDataFromBuffer(TimeseriesDataRaw timeseriesDataRawInProgress, int startIndex, int endIndex)
        {
            List<TimeseriesDataTimestamp> timestamps = new List<TimeseriesDataTimestamp>();
            TimeseriesData timeseriesData = null;

            // Rows already on buffer
            foreach (var raw in this.bufferedFrames)
            {
                timeseriesData = new TimeseriesData(raw, null, false, false);
                foreach (var timeseriesDataTimestamp in timeseriesData.Timestamps)
                {
                    timestamps.Add(timeseriesDataTimestamp);
                }
            }

            // Row on current processing TimeseriesDataRaw
            timeseriesData = new TimeseriesData(timeseriesDataRawInProgress, null, false, false);
            for(var i = startIndex; i <= endIndex; i++) 
            {
                timestamps.Add(timeseriesData.Timestamps[i]);
            }

            return new TimeseriesData(timestamps, false, false);
        }
        
        /// <summary>
        /// Flush data from the buffer and release it to make it available for Read events subscribers
        /// </summary>
        /// <param name="force">If true is flushing data even when disposed</param>
        /// <param name="includeDataInLeadingEdgeDelay">Determine whether to include data inside leading edge delay</param>
        internal void FlushData(bool force, bool includeDataInLeadingEdgeDelay = false)
        {
            if (!force && isDisposed)
            {
                throw new ObjectDisposedException(nameof(TimeseriesBuffer));
            }

            if (includeDataInLeadingEdgeDelay && bufferedFramesDelayed.Timestamps != null)
            {
                bufferedFramesDelayed.SortByTimestamp();
                bufferedFrames.Add(bufferedFramesDelayed);
                totalRowsCount += bufferedFramesDelayed.Timestamps.Length;
            }
            
            this.logger.LogTrace("Executing buffer release. Total {totalRowsCount} rows in {chunksCount} raw chunks.", this.totalRowsCount, this.bufferedFrames.Count);
            
            if (this.totalRowsCount > 0)
            {
                void RaiseData() // private function to help early return
                {
                    List<TimeseriesDataRaw> loadedData;

                    lock (this._lock)
                    {
                        if (this.totalRowsCount == 0) return; // check again in case it changes since entering the condition
                        loadedData = new List<TimeseriesDataRaw>(this.bufferedFrames);
                        if (loadedData.Count == 0) return; // this shouldn't be possible any more, but safer code won't hurt
                        this.totalRowsCount = 0;
                        this.bufferedFrames = new List<TimeseriesDataRaw>();
                    }

                    if (this.OnDataReleased == null && this.OnRawReleased == null) return;
                    
                    var newPdrw = this.ConcatDataFrames(loadedData);
                    if (this.mergeOnFlush)
                    {
                        newPdrw = this.MergeTimestamps(newPdrw);
                    }
                    
                    this.logger.LogTrace("Buffer released. After merge and clean new data contains {rows} rows.", newPdrw.Timestamps.Length);
                    
                    this.lastTimestampReleased = newPdrw.Timestamps.Last();
                    
                    if (newPdrw.Timestamps.Length <= 0) return;
                    this.InvokeOnRawReceived(this, new TimeseriesDataRawReadEventArgs(null, null, newPdrw));

                    if (this.OnDataReleased == null) return;
                    var data = new Streaming.Models.TimeseriesData(newPdrw, this.parametersFilter, false, false);
                    this.InvokeOnReceive(this, new TimeseriesDataReadEventArgs(null, null, data));
                }

                RaiseData();
            }

            lock (flushBufferTimeoutTimer)
            {
                timerEnabled = false;
                flushBufferTimeoutTimer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
            }
        }

        protected virtual void InvokeOnRawReceived(object sender, TimeseriesDataRawReadEventArgs args)
        {
            this.OnRawReleased?.Invoke(this, args);
        } 
        
        protected virtual void InvokeOnReceive(object sender, TimeseriesDataReadEventArgs args)
        {
            this.OnDataReleased?.Invoke(this, args);
        }
        
        protected virtual void InvokeOnBackill(object sender, TimeseriesDataReadEventArgs args)
        {
            this.OnBackfill?.Invoke(this, args);
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

        private TimeseriesDataRaw FilterDataFrameByFilterFunction(TimeseriesDataRaw data, Func<TimeseriesDataTimestamp, bool> filter)
        {
            var filteredRows = new List<int>();

            var timeseriesData = new Streaming.Models.TimeseriesData(data, null, false, false);
            
            // Indexes of elements which ran over the filter
            for (var i = 0; i < timeseriesData.Timestamps.Count; i++)
            {
                if (filter(timeseriesData.Timestamps[i]))
                {
                    filteredRows.Add(i);
                }
            }
            
            return this.SelectPdrRowsByMask(data, filteredRows);
        }
        
        private void UpdateLeadingEdge(TimeseriesDataRaw data)
        {
            foreach (var timestamp in data.Timestamps)
            {
                this.leadingEdge = Math.Max(this.leadingEdge, timestamp);
            }
        }

        private TimeseriesDataRaw FilterAndBackfillLateArrivingData(TimeseriesDataRaw data)
        {
            if (this.OnBackfill == null)
                return data;
            
            var indexesToBackfill = new List<int>();
            for (var i = 0; i < data.Timestamps.Length; i++)
            {
                var nano = data.Timestamps[i];
                
                if (nano <= this.lastTimestampReleased)
                {
                    indexesToBackfill.Add(i);
                }
            }

            if (!indexesToBackfill.Any()) return data;
            var backfillData = SelectPdrRowsByMask(data, indexesToBackfill);
            this.InvokeOnBackill(this, new TimeseriesDataReadEventArgs(null, null, new TimeseriesData(backfillData, parametersFilter, false, false)));
                
            var newDataIndexes = Enumerable.Range(0, data.Timestamps.Length).Except(indexesToBackfill).ToList();
            data = SelectPdrRowsByMask(data, newDataIndexes);

            return data;
        }
        
        private TimeseriesDataRaw FilterDataFrameByLeadingEdgeDelay(TimeseriesDataRaw data)
        {
            if (bufferedFramesDelayed.Timestamps != null)
            {
                data = ConcatDataFrames(new List<TimeseriesDataRaw>{bufferedFramesDelayed, data});    
            }
            
            var indexesToRelease = new List<int>();
            for (var i = 0; i < data.Timestamps.Length; i++)
            {
                var nano = data.Timestamps[i];
                
                if (this.leadingEdge - nano >= this.leadingEdgeDelayInNanoseconds)
                {
                    indexesToRelease.Add(i);
                }
            }
            
            var dataToRelease = SelectPdrRowsByMask(data, indexesToRelease);
            this.bufferedFramesDelayed = SelectPdrRowsByMask(data, Enumerable.Range(0, data.Timestamps.Length).Except(indexesToRelease).ToList());

            dataToRelease.SortByTimestamp();

            return dataToRelease;
        }

        /// <summary>
        /// Get row subset of the TimeseriesDataRaw starting from startIndex and end at startIndex + Count
        /// </summary>
        protected TimeseriesDataRaw SelectPdrRows(TimeseriesDataRaw timeseriesDataRaw, int startIndex, int count)
        {
            if (startIndex == 0 && count >= timeseriesDataRaw.Timestamps.Length)
            {
                return timeseriesDataRaw;
            }
            return this.SelectPdrRowsByMask(timeseriesDataRaw, Enumerable.Range(startIndex, count).ToList());
        }

        /// <summary>
        /// Get row subset of the TimeseriesDataRaw starting from startIndex until the last timestamp available
        /// </summary>
        protected TimeseriesDataRaw SelectPdrRows(TimeseriesDataRaw timeseriesDataRaw, int startIndex)
        {
            return SelectPdrRows(timeseriesDataRaw, startIndex, timeseriesDataRaw.Timestamps.Length - startIndex);
        }

        /// <summary>
        /// Get row subset of the TimeseriesDataRaw from a list of selected indexes
        /// </summary>
        /// <param name="timeseriesDataRaw">Original data</param>
        /// <param name="filteredRows">IEnumerable containing indexes of rows to select (e.g. [0,3,5,6])</param>
        protected TimeseriesDataRaw SelectPdrRowsByMask(TimeseriesDataRaw timeseriesDataRaw, List<int> filteredRows){
            if(filteredRows.Count() <= 0)
                return new TimeseriesDataRaw(
                    timeseriesDataRaw.Epoch,
                    new long[0],
                    new Dictionary<string, double?[]>(),
                    new Dictionary<string, string[]>(),
                    new Dictionary<string, byte[][]>(),
                    new Dictionary<string, string[]>()
                );

            // Filter all values by the filteredRows masks
            var newNumericValues = GenerateDictionaryMaskFilterMethod(timeseriesDataRaw.NumericValues, filteredRows);
            var newStringValues = GenerateDictionaryMaskFilterMethod(timeseriesDataRaw.StringValues, filteredRows);
            var newBinaryValues = GenerateDictionaryMaskFilterMethod(timeseriesDataRaw.BinaryValues, filteredRows);
            var newTagsValues = GenerateDictionaryMaskFilterMethod(timeseriesDataRaw.TagValues, filteredRows);

            return new TimeseriesDataRaw(
                timeseriesDataRaw.Epoch,
                GenerateArrayMaskFilterMethod(timeseriesDataRaw.Timestamps, filteredRows),
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
            if (originalDictionary == null) return null;
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
        /// <param name="timeseriesDataRaw">Buffer data</param>
        /// <param name="sourceIndex">Index of the timestamp to copy</param>
        /// <param name="targetIndex">Target index where to copy timestamp</param>
        protected void CopyTimeseriesDataRawIndex(TimeseriesDataRaw timeseriesDataRaw, int sourceIndex, int targetIndex)
        {
            CopyIndexWithinDataframe(timeseriesDataRaw.NumericValues, sourceIndex, targetIndex);
            CopyIndexWithinDataframe(timeseriesDataRaw.BinaryValues, sourceIndex, targetIndex);
            CopyIndexWithinDataframe(timeseriesDataRaw.StringValues, sourceIndex, targetIndex);
            CopyIndexWithinDataframe(timeseriesDataRaw.TagValues, sourceIndex, targetIndex);
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
        /// <param name="timeseriesDataRaw">Data to merge</param>
        /// <returns>New object with the proper length containing merged values</returns>
        protected TimeseriesDataRaw MergeTimestamps(TimeseriesDataRaw timeseriesDataRaw)
        {
            Dictionary<(long, long), int> timestampsDict = new Dictionary<(long, long), int>();
            int rows = timeseriesDataRaw.Timestamps.Length;
            
            long[] tagsHash = Enumerable.Repeat<long>(397, rows).ToArray();
            foreach (var keyValuePair in timeseriesDataRaw.TagValues)
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
                long timestamp = timeseriesDataRaw.Timestamps[i];
                var key = (timestamp, tagsHash[i]);
                // I have already seen this timestamp/tags in the past >> copy current row to the previous row
                if (timestampsDict.TryGetValue(key, out var value))
                {
                    this.CopyTimeseriesDataRawIndex(timeseriesDataRaw, i, value);
                }
                else
                {
                    timestampsDict.Add(key, i);
                }
            }

            if (timestampsDict.Count() == timeseriesDataRaw.Timestamps.Length)
            {
                // No filtered rows >> no need for allocating new Array
                // Since no change and no modifications were done to the original PDR
                return timeseriesDataRaw;
            }

            return this.SelectPdrRowsByMask(timeseriesDataRaw, timestampsDict.Values.ToList());
        }

        /// <summary>
        /// Concatenate list of TimeseriesDataRaws into a single TimeseriesDataRaw
        /// </summary>
        /// <param name="TimeseriesDataRaws">List of data to concatenate</param>
        /// <returns>New object with the proper length containing concatenated data</returns>
        protected TimeseriesDataRaw ConcatDataFrames(List<TimeseriesDataRaw> TimeseriesDataRaws)
        {
            if (TimeseriesDataRaws.Count == 0) return new TimeseriesDataRaw();
            long newEpoch = TimeseriesDataRaws.First().Epoch; 
            
            // Timestamps must be shifted if the epoch is different than the target epoch
            long[] newTimestamps = ArrayConcatMethod(
                TimeseriesDataRaws.Select(e =>
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
                for (var i = 0; i < TimeseriesDataRaws.Count(); i++)
                {
                    ExtendDictionaryWithKeyFilter(TimeseriesDataRaws[i].NumericValues, this.parametersFilterSet, index, rowsLen, newNumericValues);
                    ExtendDictionaryWithKeyFilter(TimeseriesDataRaws[i].BinaryValues, this.parametersFilterSet, index, rowsLen, newBinaryValues);
                    ExtendDictionaryWithKeyFilter(TimeseriesDataRaws[i].StringValues, this.parametersFilterSet, index, rowsLen, newStringValues);
                    ExtendDictionary(TimeseriesDataRaws[i].TagValues, index, rowsLen, newTagValues);
                    index += TimeseriesDataRaws[i].Timestamps.Length;
                }
            }
            else
            {
                for (var i = 0; i < TimeseriesDataRaws.Count(); i++)
                {
                    ExtendDictionary(TimeseriesDataRaws[i].NumericValues, index, rowsLen, newNumericValues);
                    ExtendDictionary(TimeseriesDataRaws[i].BinaryValues, index, rowsLen, newBinaryValues);
                    ExtendDictionary(TimeseriesDataRaws[i].StringValues, index, rowsLen, newStringValues);
                    ExtendDictionary(TimeseriesDataRaws[i].TagValues, index, rowsLen, newTagValues);
                    index += TimeseriesDataRaws[i].Timestamps.Length;
                }
            }

            var ret = new TimeseriesDataRaw(
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
            if (mergeWith == null) return;
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
        /// <param name="timeseriesDataRaw">Data to be cleaned</param>
        /// <returns>Cleaned data without the rows containing only null values</returns>
        protected TimeseriesDataRaw FilterOutNullRows(TimeseriesDataRaw timeseriesDataRaw)
        {
            // Contains only 0 and 1 values, but the datatype is in the form of byte so we can perform the xor operation under this array
            byte[] filter = Enumerable.Repeat<byte>(0, timeseriesDataRaw.Timestamps.Length).ToArray();

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

            applyFilterRows(timeseriesDataRaw.NumericValues);
            applyFilterRows(timeseriesDataRaw.BinaryValues);
            applyFilterRows(timeseriesDataRaw.StringValues);

            if (count >= timeseriesDataRaw.Timestamps.Length)
            {
                // No rows to filter
                return timeseriesDataRaw;
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


            return new TimeseriesDataRaw(
                timeseriesDataRaw.Epoch,
                AllocForFilter(timeseriesDataRaw.Timestamps),
                GenerateFilteredDictionary(timeseriesDataRaw.NumericValues),
                GenerateFilteredDictionary(timeseriesDataRaw.StringValues),
                GenerateFilteredDictionary(timeseriesDataRaw.BinaryValues),
                GenerateFilteredDictionary(timeseriesDataRaw.TagValues)
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
            this.FlushData(true, includeDataInLeadingEdgeDelay: true);
            flushBufferTimeoutTimer?.Dispose();
        }
    }
}
