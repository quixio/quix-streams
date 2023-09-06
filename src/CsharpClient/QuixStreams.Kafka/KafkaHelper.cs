using System;
using System.Linq;
using System.Text.RegularExpressions;
using Confluent.Kafka;

namespace QuixStreams.Kafka
{
    internal static class KafkaHelper
    {
        private static Regex BrokerNameChangeRegex = new Regex("Name changed from ([^ ]+) to (.+)$", RegexOptions.Compiled);
        
        public static bool TryParseBrokerNameChange(LogMessage logMessage, out string oldName, out string newName)
        {
            oldName = null;
            newName = null;
            try
            {
                // UPDATE [thrd:127.0.0.1:9092/bootstrap]: 127.0.0.1:9092/0: Name changed from 127.0.0.1:9092/bootstrap to 127.0.0.1:9092/0
                if (logMessage == null) return false;
                if (logMessage.Level != SyslogLevel.Debug) return false;
                if (!logMessage.Message.Contains("Name changed from ")) return false;
                var segments = BrokerNameChangeRegex.Match(logMessage.Message);
                if (!segments.Success) return false; // Outdated regex?
                oldName = segments.Groups[1].Value;
                newName = segments.Groups[2].Value;
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static Regex StateChangeRegex = new Regex(": ([^ ]+): Broker changed state ([a-zA-Z_]*) -> ([a-zA-Z_]*)$", RegexOptions.Compiled);

        public static bool TryParseBrokerState(LogMessage logMessage, out string broker, out string state)
        {
            broker = null;
            state = null;
            try
            {
                // Example:  Debug [thrd:sasl_ssl://IP:PORT/BROKERID]: sasl_ssl://IP:PORT/BROKERID: Broker changed state DOWN -> INIT
                if (logMessage == null) return false;
                if (logMessage.Level != SyslogLevel.Debug) return false;
                if (!logMessage.Message.Contains("Broker changed state")) return false;
                var segments = StateChangeRegex.Match(logMessage.Message);
                if (!segments.Success) return false; // Outdated regex?
                broker = segments.Groups[1].Value;
                state = segments.Groups[3].Value;
                return true;
            }
            catch
            {
                return false;
            }
        }
        
        public static bool TryParseWakeup(LogMessage logMessage, out bool readyToFetch)
        {
            readyToFetch = false;
            if (logMessage == null) return false;
            
            try
            {
                // Example:  [WAKEUP] [thrd:main]: 127.0.0.1:53631/0: Wake-up: ready to fetch
                if (logMessage.Level != SyslogLevel.Debug) return false;
                if (!string.Equals("WAKEUP", logMessage.Facility, StringComparison.InvariantCultureIgnoreCase)) return false;
                if (!logMessage.Message.Contains("Wake-up: ready to fetch")) return false;
                readyToFetch = true;
                return true;
            }
            catch (Exception ex) // left here for debugging purposes
            {
                return false;
            }
        }
        
        
        /// <summary>
        /// Determines if the kafka message is a keep alive message. This is deprecated and is no longer used. Here for backward compatibility
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        internal static bool IsKeepAliveMessage(this KafkaMessage message)
        {
            if (message.Key == null) return false;
            if (message.Key.Length != Constants.KeepAlivePackageKey.Length) return false;
            return message.Key.SequenceEqual(Constants.KeepAlivePackageKey);
        }
    }
}