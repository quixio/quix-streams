using System;
using Quix.Streams.Transport.Fw;
using Quix.Streams.Transport.Fw.Codecs;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Registry;

namespace Quix.Streams.Transport.UnitTests.Helpers
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