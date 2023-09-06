using System.Text;
using System.Text.RegularExpressions;

namespace QuixStreams.Kafka
{
    internal class Constants
    {
        public static readonly Regex ExceptionMsRegex = new Regex(" (\\d+)ms", RegexOptions.Compiled);
        public static readonly byte[] KeepAlivePackageKey = Encoding.UTF8.GetBytes("___KA___");
    }
}