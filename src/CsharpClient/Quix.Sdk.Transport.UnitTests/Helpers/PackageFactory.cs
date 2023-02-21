using System;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Registry;

namespace Quix.Sdk.Transport.UnitTests.Helpers
{
    public class PackageFactory
    {
        public static Package CreatePackage(object value, TransportContext transportContext)
        {
            CodecRegistry.RegisterCodec(new ModelKey(typeof(object)), DefaultJsonCodec.Instance);
            var producer = new Passthrough();
            Package result = null;
            producer.OnNewPackage = async package => result = package;  
            var tProducer = new TransportProducer(producer);
            tProducer.Publish(new Package<object>(new Lazy<object>(() => value), null, transportContext));
            return result;
        }
    }
}