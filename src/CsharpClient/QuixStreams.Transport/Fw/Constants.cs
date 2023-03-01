using System.Text;

namespace QuixStreams.Transport.Fw
{
    internal static class Constants
    {
        /// <summary>
        /// The message split appends markers to the beginning of the split message. 
        /// These are in the format of: &lt;SplitId/MsgIndex/MsgCount&gt;
        /// Where SplitId is integer, MsgIndex and MsgCount are a byte.
        /// In total their length is 1 (&lt;) + 4 (SplitId) + 1 (/) + 1 (MsgIndex) + 1 (/) + 1 (MsgCount) + 1 (&gt;) = 10
        /// Max SplitId = 2147483647, but even if it overflows it is fine, as the point of it is to have a unique id
        /// Max MsgIndex, MsgCount: 255, meaning if Each package is 1 MB, absolute max package size is 255MB
        /// </summary>
        internal const int MessageSeparatorInfoLength = 10;

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
        /// There is no real need for it, except it decreases the statistical chance of misidentifying a package as split package when it isn't one
        /// </summary>
        internal static readonly byte SplitSeparator = Utf8NoBOMEncoding.GetBytes("/")[0]; // This is only 1 long, so can do this!
    }
}