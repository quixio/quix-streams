using System;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.Fw.Codecs;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Registry;

namespace QuixStreams.Transport.UnitTests.Helpers
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
            tProducer.Publish(new Package<object>(value, null, transportContext));
            return result;
        }
    }
}