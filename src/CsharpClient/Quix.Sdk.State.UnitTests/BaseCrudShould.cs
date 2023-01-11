using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quix.Sdk.State.Storage;
using Quix.Sdk.State.Storage.FileStorage;
using System.Threading.Tasks;
using System.Threading;
using FluentAssertions;
using Xunit;

namespace Quix.Sdk.State.UnitTests
{
    public abstract class BaseCRUDShould
    {
        
        
        protected abstract BaseFileStorage GetStorage();

        protected async Task testLong(BaseFileStorage storage, string key, long inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetLongAsync(key);
            Assert.AreEqual(inp, ret);
        }

        protected async Task testBool(BaseFileStorage storage, string key, bool inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetBoolAsync(key);
            Assert.AreEqual(inp, ret);
        }

        protected async Task testString(BaseFileStorage storage, string key, string inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetStringAsync(key);
            Assert.AreEqual(inp, ret);
        }
        protected async Task testBinary(BaseFileStorage storage, string key, byte[] inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetBinaryAsync(key);
            Assert.AreEqual(ret.Length, inp.Length);
            for (var i = 0; i < ret.Length; i++)
            {
                Assert.AreEqual(ret[i], inp[i]);
            }
        }
        protected async Task testDouble(BaseFileStorage storage, string key, double inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetDoubleAsync(key);
            Assert.AreEqual(inp, ret);
        }
        
        [TestMethod]
        public async Task BaseCRUD_WithShorterContentThanBefore_ShouldNotThrowMalformedException()
        {
            // arrange
            var storage = this.GetStorage();
            await testString(storage, "VAL3", "12345678");
            
            // Act & Assert
            Func<Task> action = () => testString(storage, "VAL3", "123");
            
            action.Should().NotThrow<FormatException>();
        }

        [TestMethod]
        public async Task TestWriteAsyncSerial()
        {
            var storage = this.GetStorage();

            await testLong(storage, "VAL1", 1);
            await testBool(storage, "VAL2", true);
            await testString(storage, "VAL3", "ASDSADS 3123");
            await testDouble(storage, "VAL4", 1.132);
            await testBinary(storage, "VAL5", new byte[] { 1, 2, 62, 41, 5, 7, 1, 2, 9, 87, 56 });
        }

        [TestMethod]
        public void TestWriteAsyncParallel()
        {
            var storage = this.GetStorage();

            Task.WaitAll(new Task[] {
                testLong(storage, "VAL1", 1),
                testBool(storage, "VAL2", true),
                testString(storage, "VAL3", "ASDSADS 3123"),
                testDouble(storage, "VAL4", 1.132),
                testBinary(storage, "VAL5", new byte[] { 1, 2, 62, 41, 5, 7, 1, 2, 9, 87, 56 })
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(10)]
        [DataRow(500)]
        [DataRow(1024)]
        [DataRow(10 * 1024)]
        public async Task TestWriteAsyncLongBinary(int len)
        {
            var storage = this.GetStorage();

            //generate data
            var data = new byte[len];
            for (var i = 0; i < len; i++)
            {
                data[i] = (byte)(i % 256);
            }

            await testBinary(storage, $"VALBIN_{len}", data);
        }

        [TestMethod]
        public async Task TestDeleteKey()
        {
            var storage = this.GetStorage();

            await storage.SetAsync("VAL1", 1);
            await storage.SetAsync("VAL2", true);
            await storage.SetAsync("VAL3", "data");

            Assert.AreEqual((await storage.GetAllKeysAsync()).Count, 3);
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL1"));
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL2"));
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL3"));

            await storage.RemoveAsync("VAL2");
            Assert.AreEqual((await storage.GetAllKeysAsync()).Count, 2);
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL1"));
            Assert.IsFalse(await storage.ContainsKeyAsync("VAL2"));
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL3"));

            await storage.RemoveAsync("VAL3");
            Assert.AreEqual((await storage.GetAllKeysAsync()).Count, 1);
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL1"));
            Assert.IsFalse(await storage.ContainsKeyAsync("VAL2"));
            Assert.IsFalse(await storage.ContainsKeyAsync("VAL3"));

            await storage.SetAsync("VAL3", "data2");
            Assert.AreEqual((await storage.GetAllKeysAsync()).Count, 2);
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL1"));
            Assert.IsFalse(await storage.ContainsKeyAsync("VAL2"));
            Assert.IsTrue(await storage.ContainsKeyAsync("VAL3"));
        }

        [TestMethod]
        public void TestDeleteKeySync()
        {
            var storage = this.GetStorage();

            storage.Set("VAL1", 1);
            storage.Set("VAL2", true);
            storage.Set("VAL3", "data");

            Assert.AreEqual((storage.GetAllKeys()).Count, 3);
            Assert.IsTrue(storage.ContainsKey("VAL1"));
            Assert.IsTrue(storage.ContainsKey("VAL2"));
            Assert.IsTrue(storage.ContainsKey("VAL3"));

            storage.Remove("VAL2");
            Assert.AreEqual((storage.GetAllKeys()).Count, 2);
            Assert.IsTrue(storage.ContainsKey("VAL1"));
            Assert.IsFalse(storage.ContainsKey("VAL2"));
            Assert.IsTrue(storage.ContainsKey("VAL3"));

            storage.Remove("VAL3");
            Assert.AreEqual((storage.GetAllKeys()).Count, 1);
            Assert.IsTrue(storage.ContainsKey("VAL1"));
            Assert.IsFalse(storage.ContainsKey("VAL2"));
            Assert.IsFalse(storage.ContainsKey("VAL3"));

            storage.Set("VAL3", "data2");
            Assert.AreEqual((storage.GetAllKeys()).Count, 2);
            Assert.IsTrue(storage.ContainsKey("VAL1"));
            Assert.IsFalse(storage.ContainsKey("VAL2"));
            Assert.IsTrue(storage.ContainsKey("VAL3"));
        }

        [TestMethod]
        public async Task TestClearStorage()
        {
            var storage = this.GetStorage();

            await storage.SetAsync("VAL1", 1);
            await storage.SetAsync("VAL2", true);
            await storage.SetAsync("VAL3", "data");

            Assert.AreEqual((await storage.GetAllKeysAsync()).Count, 3);

            await storage.ClearAsync();
            Assert.AreEqual((await storage.GetAllKeysAsync()).Count, 0);
        }

    }
}
