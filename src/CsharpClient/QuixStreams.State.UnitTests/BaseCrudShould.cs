using System;
using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.State.Storage;
using QuixStreams.State.Storage.FileStorage;
using Xunit;

namespace QuixStreams.State.UnitTests
{
    public abstract class BaseCRUDShould
    {
        
        
        protected abstract BaseFileStorage GetStorage();

        protected async Task testLong(BaseFileStorage storage, string key, long inp)
        {
            var value = Environment.GetEnvironmentVariable("stuff");
            await storage.SetAsync(key, inp);
            var ret = await storage.GetLongAsync(key);
            ret.Should().Be(inp);
        }

        protected async Task testBool(BaseFileStorage storage, string key, bool inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetBoolAsync(key);
            ret.Should().Be(inp);
        }

        protected async Task testString(BaseFileStorage storage, string key, string inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetStringAsync(key);
            ret.Should().Be(inp);
        }
        protected async Task testBinary(BaseFileStorage storage, string key, byte[] inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetBinaryAsync(key);
            ret.Length.Should().Be(inp.Length);
            for (var i = 0; i < ret.Length; i++)
            {
                ret[i].Should().Be(inp[i]);
            }
        }
        protected async Task testDouble(BaseFileStorage storage, string key, double inp)
        {
            await storage.SetAsync(key, inp);
            var ret = await storage.GetDoubleAsync(key);
            inp.Should().Be(ret);
        }
        
        [Fact]
        public async Task BaseCRUD_WithShorterContentThanBefore_ShouldNotThrowMalformedException()
        {
            // arrange
            var storage = this.GetStorage();
            await testString(storage, "VAL3", "12345678");
            
            // Act & Assert
            Func<Task> action = () => testString(storage, "VAL3", "123");
            
            action.Should().NotThrow<FormatException>();
        }

        [Fact]
        public async Task TestWriteAsyncSerial()
        {
            var storage = this.GetStorage();

            await testLong(storage, "VAL1", 1);
            await testBool(storage, "VAL2", true);
            await testString(storage, "VAL3", "ASDSADS 3123");
            await testDouble(storage, "VAL4", 1.132);
            await testBinary(storage, "VAL5", new byte[] { 1, 2, 62, 41, 5, 7, 1, 2, 9, 87, 56 });
        }

        [Fact]
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

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(500)]
        [InlineData(1024)]
        [InlineData(10 * 1024)]
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

        [Fact]
        public async Task TestDeleteKey()
        {
            var storage = this.GetStorage();

            await storage.SetAsync("VAL1", 1);
            await storage.SetAsync("VAL2", true);
            await storage.SetAsync("VAL3", "data");

            (await storage.GetAllKeysAsync()).Length.Should().Be(3);
            (await storage.ContainsKeyAsync("VAL1")).Should().BeTrue();
            (await storage.ContainsKeyAsync("VAL2")).Should().BeTrue();
            (await storage.ContainsKeyAsync("VAL3")).Should().BeTrue();

            await storage.RemoveAsync("VAL2");
            (await storage.GetAllKeysAsync()).Length.Should().Be(2);
            (await storage.ContainsKeyAsync("VAL1")).Should().BeTrue();
            (await storage.ContainsKeyAsync("VAL2")).Should().BeFalse();
            (await storage.ContainsKeyAsync("VAL3")).Should().BeTrue();

            await storage.RemoveAsync("VAL3");
            (await storage.GetAllKeysAsync()).Length.Should().Be(1);
            (await storage.ContainsKeyAsync("VAL1")).Should().BeTrue();
            (await storage.ContainsKeyAsync("VAL2")).Should().BeFalse();
            (await storage.ContainsKeyAsync("VAL3")).Should().BeFalse();

            await storage.SetAsync("VAL3", "data2");
            (await storage.GetAllKeysAsync()).Length.Should().Be(2);
            (await storage.ContainsKeyAsync("VAL1")).Should().BeTrue();
            (await storage.ContainsKeyAsync("VAL2")).Should().BeFalse();
            (await storage.ContainsKeyAsync("VAL3")).Should().BeTrue();
        }

        [Fact]
        public void TestDeleteKeySync()
        {
            var storage = this.GetStorage();

            storage.Set("VAL1", 1);
            storage.Set("VAL2", true);
            storage.Set("VAL3", "data");

            (storage.GetAllKeys()).Length.Should().Be(3);
            (storage.ContainsKey("VAL1")).Should().BeTrue();
            (storage.ContainsKey("VAL2")).Should().BeTrue();
            (storage.ContainsKey("VAL3")).Should().BeTrue();

            storage.Remove("VAL2");
            (storage.GetAllKeys()).Length.Should().Be(2);
            (storage.ContainsKey("VAL1")).Should().BeTrue();
            (storage.ContainsKey("VAL2")).Should().BeFalse();
            (storage.ContainsKey("VAL3")).Should().BeTrue();

            storage.Remove("VAL3");
            (storage.GetAllKeys()).Length.Should().Be(1);
            (storage.ContainsKey("VAL1")).Should().BeTrue();
            (storage.ContainsKey("VAL2")).Should().BeFalse();
            (storage.ContainsKey("VAL3")).Should().BeFalse();

            storage.Set("VAL3", "data2");
            (storage.GetAllKeys()).Length.Should().Be(2);
            (storage.ContainsKey("VAL1")).Should().BeTrue();
            (storage.ContainsKey("VAL2")).Should().BeFalse();
            (storage.ContainsKey("VAL3")).Should().BeTrue();
        }

        [Fact]
        public async Task TestClearStorage()
        {
            var storage = this.GetStorage();

            await storage.SetAsync("VAL1", 1);
            await storage.SetAsync("VAL2", true);
            await storage.SetAsync("VAL3", "data");

            (await storage.GetAllKeysAsync()).Length.Should().Be(3);

            await storage.ClearAsync();
            (await storage.GetAllKeysAsync()).Length.Should().Be(0);
        }

    }
}
