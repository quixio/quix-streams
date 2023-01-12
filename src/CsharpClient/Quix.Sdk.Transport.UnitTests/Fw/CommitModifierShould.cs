using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NSubstitute;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.UnitTests.Helpers;
using Quix.TestBase.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Quix.Sdk.Transport.UnitTests.Fw
{
    public class CommitModifierShould
    {
        private readonly ITestOutputHelper helper;

        public CommitModifierShould(ITestOutputHelper helper)
        {
            Quix.Sdk.Logging.Factory = helper.CreateLoggerFactory();
            this.helper = helper;
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task Send_CommitEvery_ShouldCommitAfterAmountReached(int ackEvery)
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = ackEvery,
                CommitInterval = null
            };
            
            var modifier = new CommitModifier(commitOptions);
            var commits = new List<TransportContext[]>();
            var committer = Substitute.For<ICanCommit>();
            committer.Commit(Arg.Do<TransportContext[]>(y=> commits.Add(y)));
            modifier.Subscribe(committer);

            // Act
            for (int i = 0; i < ackEvery * 3; i++)
            {
                await modifier.Send(new Package(typeof(object), new Lazy<object>(new object())));
            }
            
            // Assert
            commits.Count.Should().Be(3);
            commits.All(x => x.Length == ackEvery).Should().BeTrue();
        }
        
        [Fact]
        public async Task Send_CommitInterval_ShouldCommitAfterTimeElapsed()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = null,
                CommitInterval = 2000
            };


            var modifier = new CommitModifier(commitOptions);
            var mre = new ManualResetEvent(false);
            var commitTimes = new List<DateTime>();
            var committer = Substitute.For<ICanCommit>();
            committer.Commit(Arg.Do<TransportContext[]>(y=>
            {
                commitTimes.Add(DateTime.UtcNow);
                mre.Set();
            }));
            modifier.Subscribe(committer);

            // Act
            var start = DateTime.UtcNow;
            await modifier.Send(new Package(typeof(object), new Lazy<object>(new object())));
            mre.WaitOne(3000);

            // Assert
            commitTimes.Count().Should().Be(1);
            var timeItTook = commitTimes[0] - start;
            // build agent doesn't can be a bit wonky, so anywhere between 1.8-3 works
            timeItTook.Should().BeLessThan(TimeSpan.FromSeconds(3));
            timeItTook.Should().BeGreaterOrEqualTo(TimeSpan.FromSeconds(1.8)); // depending on some things it might be slightly less than 2 secs
        }
        
        [Fact]
        public async Task Send_AutoCommitDisabled_ShouldNotCommit()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = false,
                CommitEvery = 1,
                CommitInterval = null
            };


            var modifier = new CommitModifier(commitOptions);
            var commits = new List<TransportContext[]>();
            var committer = Substitute.For<ICanCommit>();
            committer.Commit(Arg.Do<TransportContext[]>(y=> commits.Add(y)));
            modifier.Subscribe(committer);

            // Act
            for (int i = 0; i < 12; i++)
            {
                await modifier.Send(new Package(typeof(object), new Lazy<object>(new object())));
            }

            // Assert
            commits.Count.Should().Be(0);
        }
        
        [Fact]
        public void Commit_WithTransport_ShouldRaiseEvent()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = false
            };


            var modifier = new CommitModifier(commitOptions);
            var commits = new List<TransportContext[]>();
            var committer = Substitute.For<ICanCommit>();
            committer.Commit(Arg.Do<TransportContext[]>(y=> commits.Add(y)));
            modifier.Subscribe(committer);

            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            var transportContext2 = new TransportContext()
            {
                {"abcdef", 2}
            };

            // Act
            modifier.Commit(new [] {transportContext, transportContext2});
            

            // Assert
            commits.Count.Should().Be(1);
            commits[0].Should().BeEquivalentTo(new[] {transportContext, transportContext2});
        }
        
        [Fact]
        public void OnNewPackage_ModifierCloses_ShouldCommitOnlyCompleted()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = null,
                CommitInterval = null
            };


            var modifier = new CommitModifier(commitOptions);
            var commits = new List<TransportContext[]>();
            var committer = Substitute.For<ICanCommit>();
            committer.Commit(Arg.Do<TransportContext[]>(y=> commits.Add(y)));
            modifier.Subscribe(committer);

            var newPackageCallback = new List<(Package, TaskCompletionSource<object>)>();
            modifier.OnNewPackage = package =>
            {
                var tcs = new TaskCompletionSource<object>();
                newPackageCallback.Add((package, tcs));
                if ((int)package.TransportContext["PackageId"] == 2) tcs.SetResult(new object());
                return tcs.Task;
            };

            var package1 = PackageFactory.CreatePackage(new object(), new TransportContext() {{"PackageId", 1}});
            var package2 = PackageFactory.CreatePackage(new object(), new TransportContext() {{"PackageId", 2}});
            var package3 = PackageFactory.CreatePackage(new object(), new TransportContext() {{"PackageId", 3}});
            // Act
            modifier.Send(package1);
            modifier.Send(package2);
            modifier.Send(package3);
            modifier.Close();


            // Assert
            commits.Count.Should().Be(1);
            commits[0].Length.Should().Be(1);
            commits[0][0]["PackageId"].Should().BeEquivalentTo(2);
        }
        
        [Fact]
        public async Task OnCommit_ExceptionOccurs_ShouldNotDeadlock()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = 2,
                CommitInterval = 500
            };


            var modifier = new CommitModifier(commitOptions);
            var committer = Substitute.For<ICanCommit>();
            var revoker = Substitute.For<IRevocationPublisher>();
            var mre = new ManualResetEventSlim();
            committer.Commit(Arg.Do<TransportContext[]>(y=>
            {
                mre.Set();
                Thread.Sleep(200);
                throw new Exception("whatever");
            }));
            modifier.Subscribe(committer);
            modifier.Subscribe(revoker);

            var package1 = PackageFactory.CreatePackage(new object(), new TransportContext() {{"PackageId", 1}});
            var package2 = PackageFactory.CreatePackage(new object(), new TransportContext() {{"PackageId", 2}});
            var package3 = PackageFactory.CreatePackage(new object(), new TransportContext() {{"PackageId", 3}});
            // Act
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await Task.Run(() =>
            {
                this.helper.WriteLine(DateTime.UtcNow.ToString("G") + "- Wait 1");
                modifier.Send(package1);
                modifier.Send(package2);
                mre.Wait();
                mre.Reset();
                modifier.Send(package3);
                this.helper.WriteLine(DateTime.UtcNow.ToString("G") + "- Wait 2");
                mre.Wait();
                this.helper.WriteLine(DateTime.UtcNow.ToString("G") + "- After wait 2");
                revoker.OnRevoked += Raise.Event<EventHandler<OnRevokedEventArgs>>(revoker, new OnRevokedEventArgs());
            }, cts.Token);

            // Assert
            cts.IsCancellationRequested.Should().BeFalse();
        }
    }
}