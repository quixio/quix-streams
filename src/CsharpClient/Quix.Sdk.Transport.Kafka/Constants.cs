using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport.Kafka
{
    internal class Constants
    {
        public static readonly Regex ExceptionMsRegex = new Regex(" (\\d+)ms", RegexOptions.Compiled);
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
            
            KeepAlivePackage.SetKey("___KA___");
        }
    }
}