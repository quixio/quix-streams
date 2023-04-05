using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.IO;
using Xunit;

namespace QuixStreams.Transport.UnitTests.Fw
{
    public class ByteMergingModifierShould
    {
        [Fact]
        public void Modify_MergeReturnsBytes_ShouldRaisePackageAndReturnCompletedTask()
        {
            // Arrange
            var random = new Random();
            var bytes = new byte[500];
            random.NextBytes(bytes);
            var metaData = new MetaData(new Dictionary<string, string> {{"Test", "123"}});
            var transportContext = new TransportContext(new Dictionary<string, object> {{"test", 123}});
            var package = new Package<byte[]>(bytes, metaData, transportContext);
            var merger = Substitute.For<IByteMerger>();
            merger.Merge(Arg.Any<byte[]>(), Arg.Any<string>(), out Arg.Any<string>()).ReturnsForAnyArgs(bytes);
            var modifier = new ByteMergingModifier(merger);

            Package nonGeneric = null;
            modifier.OnNewPackage = (Package args) =>
            {
                nonGeneric = args;
                return Task.CompletedTask;
            };

            // Act
            var task = modifier.Publish(package);

            // Assert
            var sw = Stopwatch.StartNew();
            task.Wait(2000);
            sw.Stop();
            sw.Elapsed.TotalMilliseconds.Should().BeLessThan(50); // I'm giving Task Library 50 ms to get its act together and call what it needs to. Usually takes ~1-2ms
            task.IsCompleted.Should().BeTrue();
            nonGeneric.Should().NotBeNull();
            ((byte[])nonGeneric.Value).Should().BeEquivalentTo(package.Value);
            nonGeneric.MetaData.Should().BeEquivalentTo(package.MetaData);
            nonGeneric.TransportContext.Should().BeEquivalentTo(package.TransportContext);
        }
        
        [Fact]
        public async Task Modify_SplitPackageMerges_ShouldHaveTransportContextOfFirstPackage()
        {
            // Reason behind this is: When the transport context contains information such as offset the package is from,
            // then if you have following scenario:
            // [Package1_segment1 offset 43] [Package2 offset 44] [package1_segment2 offset 45]
            // then because of what the package raise order is (package1, package2, see other tests), if you were to 
            // assign offset 45 to package 1, commit it and then crash, you would lose out on package 2.
            // Therefore one should instead commit offset 43, so package1 is "handled",
            // (you wouldn't have the segments to re-assamble it again), without losing package 2
            
            // Arrange
            var merger = Substitute.For<IByteMerger>();
            // P1_s1
            var p1s1 = new Package<byte[]>(new byte[] {1});
            p1s1.TransportContext["Package"] = 1;
            merger.Merge(p1s1.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return (byte[]) null;
            });
            // P1_s2
            var p1s2 = new Package<byte[]>(new byte[] {2});
            p1s2.TransportContext["Package"] = 2;
            // p1_merged
            var p1merged = new Package<byte[]>(new byte[] {3});
            merger.Merge(p1s2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return p1merged.Value;
            });
            
            var modifier = new ByteMergingModifier(merger);

            Package receivedPackage = null;
            modifier.OnNewPackage = (Package arg) =>
            {
                receivedPackage = arg;
                return Task.CompletedTask;
            };

            // Act
            await modifier.Publish(p1s1);
            await modifier.Publish(p1s2);

            // Assert
            receivedPackage.Should().NotBeNull();
            receivedPackage.TransportContext["Package"].Should().Be(1);
        }
        
        [Fact]
        public async Task Modify_SplitPackageInterweavedWithOtherPackages_ShouldRaiseInExpectedOrder()
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Package1_segment1] [Package2_segment1] [Package3] [Package1_segment2] [Package2_segment2]
            // then the outcome is [Package1_merged] [Package2_merged] [Package3]
            
            // Arrange
            var merger = Substitute.For<IByteMerger>();
            // P1_s1
            var p1s1 = new Package<byte[]>(new byte[] {1});
            merger.Merge(p1s1.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return (byte[]) null;
            });
            // P1_s2
            var p1s2 = new Package<byte[]>(new byte[] {2});
            // p1_merged
            var p1merged = new Package<byte[]>(new byte[] {3});
            merger.Merge(p1s2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return p1merged.Value;
            });
            // P2_s1
            var p2s1 = new Package<byte[]>(new byte[] {4});
            merger.Merge(p2s1.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p2";
                return (byte[]) null;
            });
            // P2_s2
            var p2s2 = new Package<byte[]>(new byte[] {5});
            // p2_merged
            var p2merged = new Package<byte[]>(new byte[] {6});
            merger.Merge(p2s2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p2";
                return p2merged.Value;
            });
            // P3
            var p3 = new Package<byte[]>(new byte[] {7});
            merger.Merge(p3.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = null;
                return p3.Value;
            });


            var expectedOrder = new List<byte[]>()
            {
                p1merged.Value,
                p2merged.Value,
                p3.Value
            };
            
            var modifier = new ByteMergingModifier(merger);

            var packagesReceived = new List<Package>();
            modifier.OnNewPackage = (Package args) =>
            {
                packagesReceived.Add(args);
                return Task.CompletedTask;
            };

            // Act
            await modifier.Publish(p1s1);
            packagesReceived.Count().Should().Be(0); // only segments so far
            await modifier.Publish(p2s1);
            packagesReceived.Count().Should().Be(0); // only segments so far
            await modifier.Publish(p3);
            packagesReceived.Count().Should().Be(0); // have a normal package, but in-between segments
            await modifier.Publish(p1s2);
            packagesReceived.Count().Should().Be(1); // first package segments arrived, but the normal package is still between segments
            await modifier.Publish(p2s2);
            packagesReceived.Count().Should().Be(3); // the last segment arrived for the package wrapping the normal package, so both the normal and the merged release

            // Assert
            var actualOrder = packagesReceived.Select(x => x.Value).ToList();
            actualOrder.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
        }
        
        [Fact]
        public void Modify_SplitPackageInterweavedWithOtherAndSplitPackageExpires_ShouldRaiseInExpectedOrder()
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Package1_segment1] [Package2] [Package1_segment2]  but package1 expires before package1_segment2 arrived 
            // then the outcome is [Package2]
            
            // Arrange
            var merger = Substitute.For<IByteMerger>();
            // P1_s1
            var p1s1 = new Package<byte[]>(new byte[] {1});
            merger.Merge(p1s1.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return (byte[]) null;
            });
            // P2
            var p2 = new Package<byte[]>(new byte[] {4});
            merger.Merge(p2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = null;
                return p2.Value;
            });


            var expectedOrder = new List<byte[]>()
            {
                p2.Value,
            };
            
            var modifier = new ByteMergingModifier(merger);

            var packagesReceived = new List<Package>();
            modifier.OnNewPackage = (Package args) =>
            {
                packagesReceived.Add(args);
                return Task.CompletedTask;
            };

            // Act
            var tasks = new List<Task>(); 
            tasks.Add(modifier.Publish(p1s1));
            merger.OnMessageSegmentsPurged += Raise.Event<Action<string>>("p1"); // IMPORTANT! this is before p2
            tasks.Add(modifier.Publish(p2));
            Task.WaitAll(tasks.ToArray(), 2000);

            // Assert
            tasks.All(x=> x.IsCompleted).Should().BeTrue();
            packagesReceived.Count().Should().Be(1);
            var actualOrder = packagesReceived.Select(x => x.Value).ToList();
            actualOrder.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
        }
        
        [Fact]
        public void Modify_SplitPackageInterweavedWithOtherAndSplitPackageExpiresAfterNonSplit_ShouldRaiseInExpectedOrder()
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Package1_segment1] [Package2] but package1 expires before package1_segment2 arrived 
            // then the outcome is [Package2]
            
            // Arrange
            var merger = Substitute.For<IByteMerger>();
            // P1_s1
            var p1s1 = new Package<byte[]>(new byte[] {1});
            merger.Merge(p1s1.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return (byte[]) null;
            });
            // P2
            var p2 = new Package<byte[]>(new byte[] {4});
            merger.Merge(p2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = null;
                return p2.Value;
            });


            var expectedOrder = new List<byte[]>()
            {
                p2.Value,
            };
            
            var modifier = new ByteMergingModifier(merger);

            var packagesReceived = new List<Package>();
            modifier.OnNewPackage = (Package args) =>
            {
                packagesReceived.Add(args);
                return Task.CompletedTask;
            };

            // Act
            var tasks = new List<Task>(); 
            tasks.Add(modifier.Publish(p1s1));
            tasks.Add(modifier.Publish(p2));
            merger.OnMessageSegmentsPurged += Raise.Event<Action<string>>("p1"); // IMPORTANT! this is after p2
            Task.WaitAll(tasks.ToArray(), 2000);

            // Assert
            tasks.All(x=> x.IsCompleted).Should().BeTrue();
            packagesReceived.Count().Should().Be(1);
            var actualOrder = packagesReceived.Select(x => x.Value).ToList();
            actualOrder.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
        }

        [Fact]
        public void Modify_MergeReturnsNull_ShouldNotRaisePackageAndReturnCompletedTask()
        {
            // Arrange
            var random = new Random();
            var bytes = new byte[500];
            random.NextBytes(bytes);
            var metaData = new MetaData(new Dictionary<string, string> { { "Test", "123" } });
            var transportContext = new TransportContext(new Dictionary<string, object> { { "test", 123 } });
            var package = new Package<byte[]>(bytes, metaData, transportContext);
            var merger = Substitute.For<IByteMerger>();
            merger.Merge(Arg.Any<byte[]>(), Arg.Any<string>(), out Arg.Any<string>()).ReturnsForAnyArgs((byte[])null);
            var modifier = new ByteMergingModifier(merger);

            Package nonGeneric = null;
            modifier.OnNewPackage = (Package args) =>
            {
                nonGeneric = args;
                return Task.CompletedTask;
            };

            // Act
            var task = modifier.Publish(package);

            // Assert
            var sw = Stopwatch.StartNew();
            task.Wait(2000);
            sw.Stop();
            sw.Elapsed.TotalMilliseconds.Should().BeLessThan(50); // I'm giving Task Library 50 ms to get its act together and call what it needs to. Usually takes ~1-2ms
            task.IsCompleted.Should().BeTrue();
            nonGeneric.Should().BeNull();
        }
        
        [Fact]
        public void Modify_SplitPackageInterweavedWithOtherAndSplitPackageRevoked_ShouldDiscardRevokedAndRaiseInExpectedOrder()
        {
            // This is a bit of complex text. The idea is that if you have the following data to be merged:
            // [Package1_segment1] [Package2] [Package1_segment2] [Package3] but source gets revoked and causes Package1 to
            // disappear then should raise [Package2] and [Package3]
            
            // Arrange
            var merger = Substitute.For<IByteMerger>();
            // P1_s1
            var p1s1 = new Package<byte[]>(new byte[] {1}, transportContext: new TransportContext(new Dictionary<string, object>()
            {
                {"Package", "1"},
                {"Segment", "1"}
            }));
            merger.Merge(p1s1.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return (byte[]) null;
            });
            // P2
            var p2 = new Package<byte[]>(new byte[] {1}, transportContext: new TransportContext(new Dictionary<string, object>()
            {
                {"Package", "2"}
            }));
            merger.Merge(p2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = null;
                return p2.Value;
            });
            // P1_s2
            var p1s2 = new Package<byte[]>(new byte[] {3}, transportContext: new TransportContext(new Dictionary<string, object>()
            {
                {"Package", "1"},
                {"Segment", "2"}
            }));
            merger.Merge(p1s2.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = "p1";
                return (byte[]) null;
            });
            // P3
            var p3 = new Package<byte[]>(new byte[] {4}, transportContext: new TransportContext(new Dictionary<string, object>()
            {
                {"Package", "3"},
            }));
            merger.Merge(p3.Value, Arg.Any<string>(), out Arg.Any<string>()).Returns(x=>
            {
                x[2] = null;
                return p3.Value;
            });


            var expectedOrder = new List<byte[]>()
            {
                p2.Value,
                p3.Value,
            };
            
            var modifier = new ByteMergingModifier(merger);
            var revocationPublisher = Substitute.For<IRevocationPublisher>();
            modifier.Subscribe(revocationPublisher);
            revocationPublisher.FilterRevokedContexts(Arg.Any<object>(), Arg.Any<IEnumerable<TransportContext>>()).Returns(new[] {p1s1.TransportContext, p1s2.TransportContext});

            var packagesReceived = new List<Package>();
            modifier.OnNewPackage = (Package args) =>
            {
                packagesReceived.Add(args);
                return Task.CompletedTask;
            };

            // Act
            var tasks = new List<Task>(); 
            tasks.Add(modifier.Publish(p1s1));
            tasks.Add(modifier.Publish(p2));
            tasks.Add(modifier.Publish(p1s2));
            tasks.Add(modifier.Publish(p3));
            revocationPublisher.OnRevoked += Raise.EventWith(new OnRevokedEventArgs());

            // Assert
            tasks.All(x=> x.IsCompleted).Should().BeTrue();
            packagesReceived.Count().Should().Be(2);
            var actualOrder = packagesReceived.Select(x => x.Value).ToList();
            actualOrder.Should().BeEquivalentTo(expectedOrder, o => o.WithStrictOrdering());
            merger.Received(1).Purge("p1");
            merger.Received(1).Purge(Arg.Any<string>()); // the non-fragments shouldn't get purged - and cause exceptions in merger -
        }
    }
}