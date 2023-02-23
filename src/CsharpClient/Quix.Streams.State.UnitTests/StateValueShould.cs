using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Quix.Streams.State.UnitTests
{
    [TestClass]
    public class StateValueShould
    {
        [TestMethod]
        public void TestLong()
        {
            var value = new StateValue(1);
            Assert.AreEqual(value.Type, StateValue.StateType.Long);
            Assert.AreEqual(value.LongValue, 1);
        }

        [TestMethod]
        public void TestBool()
        {
            var value = new StateValue(true);
            Assert.AreEqual(value.Type, StateValue.StateType.Bool);
            Assert.AreEqual(value.BoolValue, true);
        }
        [TestMethod]
        public void TestString()
        {
            var value = new StateValue("TestStr312");
            Assert.AreEqual(value.Type, StateValue.StateType.String);
            Assert.AreEqual(value.StringValue, "TestStr312");
        }
        [TestMethod]
        public void TestDouble()
        {
            var value = new StateValue(0.426);
            Assert.AreEqual(value.Type, StateValue.StateType.Double);
            Assert.AreEqual(value.DoubleValue, 0.426);
        }
        [TestMethod]
        public void TestBinary()
        {
            var bytes = new byte[] { 0, 4, 6, 8, 1, 0, 43, 255, 0, 32 };

            var value = new StateValue(bytes);
            Assert.AreEqual(value.Type, StateValue.StateType.Binary);
            Assert.AreEqual(value.BinaryValue.Length, bytes.Length);

            for (var i = 0; i < bytes.Length; i++)
            {
                Assert.AreEqual(value.BinaryValue[i], bytes[i]);
            }
        }


    }
}
