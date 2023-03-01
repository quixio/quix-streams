using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.IO;
using Xunit;

namespace QuixStreams.Transport.UnitTests.Fw
{
    public class ByteSplittingModifierShould
    {
        [Fact]
        public void Modify_ByteSplitDoesntHappen_ShouldRaisePackage()
        {
            // Arrange 
            var random = new Random();
            var bytes = new byte[500];
            random.NextBytes(bytes);
            var metaData = new MetaData(new Dictionary<string, string> { { "Test", "123" } });
            var transportContext = new TransportContext(new Dictionary<string, object> { { "test", 123 } });
            var value = new Lazy<byte[]>(bytes);
            var package = new Package<byte[]>(value, metaData, transportContext);
            var splitter = Substitute.For<IByteSplitter>();
            splitter.Split(Arg.Any<byte[]>()).ReturnsForAnyArgs(new[] { bytes });
            var modifier = new ByteSplittingModifier(splitter);

            var nonGeneric = new List<Package>();
            modifier.OnNewPackage = (Package args) =>
            {
                nonGeneric.Add(args);
                return Task.CompletedTask;
            };

            // Act
            var task = modifier.Publish(package);

            // Assert
            task.Wait(2000);
            nonGeneric.Count.Should().Be(1, "No splitting should be done");
            var raisedPackage = nonGeneric[0];
            raisedPackage.Value.Value.Should().BeEquivalentTo(package.Value.Value);
            raisedPackage.MetaData.Should().BeEquivalentTo(package.MetaData);
            raisedPackage.TransportContext.Should().BeEquivalentTo(package.TransportContext);
        }

        [Fact]
        public void Modify_ByteSplitDoesHappen_ShouldRaisePackagesAndCompletedTask()
        {
            // Arrange 
            var random = new Random();
            var bytes = new byte[500];
            random.NextBytes(bytes);
            var metaData = new MetaData(new Dictionary<string, string> { { "Test", "123" } });
            var transportContext = new TransportContext(new Dictionary<string, object> { { "test", 123 } });
            var value = new Lazy<byte[]>(bytes);
            var package = new Package<byte[]>(value, metaData, transportContext);
            var splitter = Substitute.For<IByteSplitter>();
            var returned = new List<byte[]>();
            const int splitCount = 5;
            for (var i = 0; i < splitCount; i++)
            {
                var segmentBytes = new byte[111];
                random.NextBytes(segmentBytes);
                returned.Add(segmentBytes);
            }
            splitter.Split(Arg.Any<byte[]>()).ReturnsForAnyArgs(returned);
            var modifier = new ByteSplittingModifier(splitter);

            var nonGeneric = new List<Package>();
            modifier.OnNewPackage = (Package args) =>
            {
                nonGeneric.Add(args);
                return Task.CompletedTask;
            };

            // Act
            var task = modifier.Publish(package);

            // Assert
            task.Wait(2000);
            task.IsCompleted.Should().BeTrue();
            nonGeneric.Count.Should().Be(splitCount, "Expected number of splits should be performed");
            for (var index = 0; index < nonGeneric.Count - 1; index++)
            {
                var raisedPackage = nonGeneric[index];
                raisedPackage.Value.Value.Should().BeEquivalentTo(returned[index]);
                raisedPackage.MetaData.Should().BeEquivalentTo(MetaData.Empty);
                raisedPackage.TransportContext.Should().BeEquivalentTo(package.TransportContext);
            }

            var lastRaisedPackage = nonGeneric[nonGeneric.Count - 1];
            lastRaisedPackage.Value.Value.Should().BeEquivalentTo(returned[nonGeneric.Count - 1]);
            lastRaisedPackage.MetaData.Should().BeEquivalentTo(package.MetaData);
            lastRaisedPackage.TransportContext.Should().BeEquivalentTo(package.TransportContext);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10, Skip = "No implicit support for multiple subscribers out of box for now.")]
        public void Send_SubscribersTakeTimeToCompleteTask_ReturnedTaskShouldOnlyResolveWhenAllPackageTaskResolve(int packageCount)
        {
            // Arrange
            var random = new Random();
            var bytes = new byte[500];
            random.NextBytes(bytes);
            var value = new Lazy<byte[]>(bytes);
            var package = new Package<byte[]>(value);
            var splitter = Substitute.For<IByteSplitter>();
            var returned = new List<byte[]>();
            for (var i = 0; i < packageCount; i++)
            {
                var segmentBytes = new byte[111];
                random.NextBytes(segmentBytes);
                returned.Add(segmentBytes);
            }
            splitter.Split(Arg.Any<byte[]>()).ReturnsForAnyArgs(returned);
            var modifier = new ByteSplittingModifier(splitter);

            var manResetEvents = new List<ManualResetEvent>(packageCount);
            for (int index = 0; index < packageCount; index++)
            {
                manResetEvents.Add(new ManualResetEvent(false));
            }
            var packageCounter = 0;
            modifier.OnNewPackage = (args =>
            {
                var packageIndex = Interlocked.Increment(ref packageCounter) - 1;
                var taskSource = new TaskCompletionSource<object>();
                Task.Run(() =>
                {
                    manResetEvents[packageIndex].WaitOne();
                    taskSource.SetResult(null);
                });
                return taskSource.Task;
            });

            // Act
            var task = modifier.Publish(package);

            // Assert
            task.IsCompleted.Should().BeFalse("Because manual reset events are still waiting to be set");
            for (int i = 0; i < packageCount - 1; i++)
            {
                manResetEvents[i].Set();
                Thread.Sleep(20); // I'm giving Task Library a bit of time to resolve itself, in case it is resolved (it really shouldn't, but if it does, I want to know)
                task.IsCompleted.Should().BeFalse($"Because only {i + 1} out of {packageCount} manual reset events are set");
            }
            manResetEvents[packageCount - 1].Set();
            var sw = Stopwatch.StartNew();
            task.Wait(2000);
            sw.Stop();
            sw.Elapsed.TotalMilliseconds.Should().BeLessThan(500); // I'm giving Task Library 500 ms to get its act together and call what it needs to. Usually takes ~1-2ms
            task.IsCompleted.Should().BeTrue();
        }
    }
}
