using System;
using System.Linq;
using System.Text;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Kafka.Transport.Tests.Helpers
{
    public class TestModel : IEquatable<TestModel>
    {
        static TestModel()
        {
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(typeof(TestModel), new DefaultJsonCodec<TestModel>());
            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(typeof(TestModel[]), new DefaultJsonCodec<TestModel[]>());
        }

        public string StringProp { get; set; }
        public int IntProp { get; set; }

        public byte[] ByteArray { get; set; }


        public static TestModel Create(int size = 15)
        {
            var p = new TestModel()
            {
                ByteArray = new byte[size]
            };
            var random = new Random();
            random.NextBytes(p.ByteArray);

            p.StringProp = Encoding.ASCII.GetString(p.ByteArray);
            p.IntProp = p.ByteArray[3] ^ p.ByteArray[5];
            return p;
        }

        public bool Equals(TestModel other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return this.StringProp == other.StringProp && this.IntProp == other.IntProp && (Equals(this.ByteArray, other.ByteArray) || this.ByteArray.SequenceEqual(other.ByteArray));
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return this.Equals((TestModel)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (this.StringProp != null ? this.StringProp.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ this.IntProp;
                hashCode = (hashCode * 397) ^ (this.ByteArray != null ? this.ByteArray.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}