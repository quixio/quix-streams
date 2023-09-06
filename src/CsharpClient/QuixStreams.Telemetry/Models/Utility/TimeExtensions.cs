using System;

namespace QuixStreams.Telemetry.Models.Utility
{
    /// <summary>
    /// Time utility extensions for Epoch, DateTime and Nanoseconds manipulation
    /// </summary>
    public static class TimeExtensions
    {
        public static readonly DateTime UnixEpoch = new DateTime(1970, 01, 01);

        /// <summary>
        /// Converts datetime to nanoseconds since unix epoch (01/01/1970)
        /// </summary>
        /// <param name="dateTime">The datetime to convert</param>
        /// <returns>Datetime in nanoseconds since unix epoch</returns>
        public static long ToUnixNanoseconds(this DateTime dateTime)
        {
            return (dateTime - UnixEpoch).ToNanoseconds();
        }


        /// <summary>
        /// Converts the timespan to nanoseconds
        /// </summary>
        /// <param name="timespan">the timespan to convert</param>
        /// <returns>Timespan in nanoseconds</returns>
        public static long ToNanoseconds(this TimeSpan timespan)
        {
            return timespan.Ticks * 100;
        }

        /// <summary>
        /// Converts the nanoseconds to timespan
        /// </summary>
        /// <param name="timestamp">The nanoseconds to convert</param>
        /// <returns>Converted timespan</returns>
        public static TimeSpan FromNanoseconds(this long timestamp)
        {
            return new TimeSpan(timestamp / 100);
        }

        /// <summary>
        /// Converts unix epoch nanoseconds to datetime
        /// </summary>
        /// <param name="timestamp">The unix nanoseconds to convert</param>
        /// <returns>Converted datetime</returns>
        public static DateTime FromUnixNanoseconds(this long timestamp)
        {
            return UnixEpoch + timestamp.FromNanoseconds();
        }
    }
}