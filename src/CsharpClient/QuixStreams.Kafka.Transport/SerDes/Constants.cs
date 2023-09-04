using System.Text;

namespace QuixStreams.Kafka.Transport.SerDes
{
    internal static class Constants
    {
        /// <summary>
        /// The message split appends markers to the beginning of the split message. 
        /// These are in the format of: &lt;SplitId/MsgIndex/MsgCount&gt;
        /// Where SplitId is integer, MsgIndex and MsgCount are a byte.
        /// In total their length is 1 (&lt;) + 4 (SplitId) + 1 (/) + 1 (MsgIndex) + 1 (/) + 1 (MsgCount) + 1 (&gt;) = 10
        /// Max SplitId = 2147483647, but even if it overflows it is fine, as the point of it is to have a unique id
        /// Max MsgIndex, MsgCount: 255, meaning if Each transportPackage is 1 MB, absolute max transportPackage size is 255MB
        /// </summary>
        internal const int LegacyMessageSeparatorInfoLength = 10;

        /// <summary>
        /// UTF 8 encoding without the Byte Order Marker.
        /// </summary>
        internal static readonly Encoding Utf8NoBOMEncoding = new UTF8Encoding(false, false);

        /// <summary>
        /// The message split appends markers to the beginning of the split message. This is the beginning of such a marker.
        /// </summary>
        internal static readonly byte SplitStart = Utf8NoBOMEncoding.GetBytes("<")[0]; // This is only 1 long, so can do this!

        /// <summary>
        /// The message split appends markers to the beginning of the split message. This is the end of such a marker.
        /// </summary>
        internal static readonly byte SplitEnd = Utf8NoBOMEncoding.GetBytes(">")[0]; // This is only 1 long, so can do this!

        /// <summary>
        /// The message split appends markers to the beginning of the split message. This is the separator between values of such markers.
        /// There is no real need for it, except it decreases the statistical chance of misidentifying a transportPackage as split transportPackage when it isn't one
        /// </summary>
        internal static readonly byte SplitSeparator = Utf8NoBOMEncoding.GetBytes("/")[0]; // This is only 1 long, so can do this!

        internal const string KafkaMessageHeaderModelKey = "__Q_ModelKey";
        internal const string KafkaMessageHeaderCodecId = "__Q_CodecId";
        
        /// <summary>
        /// The unique identifier the split message
        /// </summary>
        internal const string KafkaMessageHeaderSplitMessageId = "__Q_SplitMessageId";
        
        /// <summary>
        /// The message segment position starting from 0 index
        /// </summary>
        internal const string KafkaMessageHeaderSplitMessageIndex = "__Q_SplitMessageIndex";
        
        /// <summary>
        /// The message segment count the message got split into
        /// </summary>
        internal const string KafkaMessageHeaderSplitMessageCount = "__Q_SplitMessageCount";


    }
}