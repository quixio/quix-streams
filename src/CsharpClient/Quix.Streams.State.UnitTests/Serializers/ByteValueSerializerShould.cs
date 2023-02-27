using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quix.Streams.State.Serializers;

namespace Quix.Streams.State.UnitTests.Serializers
{
    [TestClass]
    public class ByteValueSerializerShould
    {

        [TestMethod]
        [ExpectedException(typeof(FormatException),
            "Wrong codec id")]
        public void TestFalsy()
        {
            byte[] data = new byte[] { (byte)'c', 1, 2 };
            ByteValueSerializer.Deserialize(data);
        }

        [TestMethod]
        public void TestBoolean()
        {
            var data = new StateValue(true);
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            Assert.IsTrue(data.Equals(deserialized));
        }

        [TestMethod]
        public void TestString()
        {
            var data = new StateValue("123");
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            Assert.IsTrue(data.Equals(deserialized));
        }

        [TestMethod]
        public void TestLong()
        {
            var data = new StateValue(12L);
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            Assert.IsTrue(data.Equals(deserialized));
        }

        [TestMethod]
        public void TestBinary()
        {
            var data = new StateValue(new byte[] { 1,5,78,21 });
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            Assert.IsTrue(data.Equals(deserialized));
        }

        [TestMethod]
        public void TestDouble()
        {
            var data = new StateValue(1.57);
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            Assert.IsTrue(data.Equals(deserialized));
        }

    }
}
