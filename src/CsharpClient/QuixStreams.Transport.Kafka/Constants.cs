using System;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Kafka
{
    internal class Constants
    {
        public static readonly Regex ExceptionMsRegex = new Regex(" (\\d+)ms", RegexOptions.Compiled);
        public static readonly byte[] KeepAlivePackageKey = Encoding.UTF8.GetBytes("___KA___");
        public static Package KeepAlivePackage;
        

        static Constants()
        {
            var serializingModifier = new SerializingModifier();
            serializingModifier.OnNewPackage += package =>
            {
                KeepAlivePackage = package;
                return Task.CompletedTask;
            };

            serializingModifier.Send(new Package<string>(new Lazy<string>(() => "")));
            
            Debug.Assert(KeepAlivePackage != null);
            
            KeepAlivePackage.SetKey(KeepAlivePackageKey);
        }
    }
}