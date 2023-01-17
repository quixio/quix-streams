﻿using System;
using System.Collections.Generic;
using System.Linq;
using Quix.Sdk.Transport.IO;
using Newtonsoft.Json;

namespace Quix.Sdk.Process.Models
{
    /// <summary>
    /// Describes parameter data for multiple timestamps
    /// </summary>
    [ModelKey("ParameterData")]
    public class ParameterDataRaw
    {

        /// <summary>
        /// Create a new empty Parameter Data Raw instance
        /// </summary>
        public ParameterDataRaw()
        {
            this.NumericValues = new Dictionary<string, double?[]>();
            this.StringValues = new Dictionary<string, string[]>();
            this.BinaryValues = new Dictionary<string, byte[][]>();
        }

        /// <summary>
        /// Create a new Parameter Data Raw instance with predefined values
        /// </summary>
        public ParameterDataRaw(
            long Epoch, 
            long[] Timestamps,
            Dictionary<string, double?[]> NumericValues,
            Dictionary<string, string[]> StringValues,
            Dictionary<string, byte[][]> BinaryValues,
            Dictionary<string, string[]> TagValues
        )
        {
            this.Epoch = Epoch;
            this.Timestamps = Timestamps;
            this.NumericValues = NumericValues;
            this.StringValues = StringValues;
            this.BinaryValues = BinaryValues;
            this.TagValues = TagValues;
        }

        /// <summary>
        /// The unix epoch from, which all other timestamps in this model are measured from in nanoseconds.
        /// 0 = UNIX epoch (01/01/1970)
        /// </summary>
        public long Epoch;

        /// <summary>
        /// The timestamps of values in nanoseconds since <see cref="Epoch"/>.
        /// Timestamps are matched by index to <see cref="NumericValues"/>, <see cref="StringValues"/>, <see cref="BinaryValues"/>and <see cref="TagValues"/>
        /// </summary>
        public long[] Timestamps;

        /// <summary>
        /// The numeric values of parameters.
        /// The key is the parameter Id the values belong to
        /// Numerical values of the corresponding parameter. Values are matched by index to <see cref="Timestamps"/>
        /// </summary>
        public Dictionary<string, double?[]> NumericValues;

        /// <summary>
        /// The string values for parameters.
        /// The key is the parameter Id the values belong to
        /// String values of the corresponding parameter. Values are matched by index to <see cref="Timestamps"/>
        /// </summary>
        public Dictionary<string, string[]> StringValues;

        /// <summary>
        /// The binary values for parameters.
        /// The key is the parameter Id the values belong to
        /// Binary values of the corresponding parameter. Values are matched by index to <see cref="Timestamps"/>
        /// </summary>
        public Dictionary<string, byte[][]> BinaryValues;

        /// <summary>
        /// The tag values for each timestamp.
        /// The key is the tag Id
        /// Tag values for each timestamp. Values are matched by index to <see cref="Timestamps"/>
        /// </summary>
        public Dictionary<string, string[]> TagValues;

        /// <summary>
        /// Returns the Json representation of the object
        /// </summary>
        /// <returns>Json string</returns>
        public string ToJson()
        {
            JsonSerializerSettings settings = new JsonSerializerSettings();
            settings.Formatting = Formatting.Indented;

            return JsonConvert.SerializeObject(this, settings);
        }
    }

    /// <summary>
    /// Extensions methods for ParameterDataRaw
    /// </summary>
    public static class ParameterDataRawExtensions
    {
        /// <summary>
        /// Clone ParameterDataRaw instance
        /// </summary>
        /// <param name="rawData">ParameterDataRaw to clone</param>
        /// <returns>Cloned instance</returns>
        public static ParameterDataRaw Clone(this ParameterDataRaw rawData)
        {
            var result = new ParameterDataRaw()
            {
                Epoch = rawData.Epoch,
                Timestamps = (long[])rawData.Timestamps.Clone(),
                NumericValues = rawData.NumericValues.ToDictionary(kv => kv.Key, kv => (double?[])kv.Value.Clone()),
                StringValues = rawData.StringValues.ToDictionary(kv => kv.Key, kv => (string[])kv.Value.Clone()),
                BinaryValues = rawData.BinaryValues.ToDictionary(kv => kv.Key, kv => (byte[][])kv.Value.Clone()),
                TagValues = rawData.TagValues.ToDictionary(kv => kv.Key, kv => (string[])kv.Value.Clone()),
            };
            return result;
        }
    }
}