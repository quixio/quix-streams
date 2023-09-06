using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using Quix.TestBase.Extensions;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Kafka.Transport.Tests
{
    public class CommitModifierShould
    {
        private readonly ITestOutputHelper helper;

        public CommitModifierShould(ITestOutputHelper helper)
        {
            QuixStreams.Logging.Factory = helper.CreateLoggerFactory();
            this.helper = helper;
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task Publish_CommitEvery_ShouldCommitAfterAmountReached(int ackEvery)
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = ackEvery,
                CommitInterval = null
            };
            
            var commits = new List<ICollection<TopicPartitionOffset>>();
            var autoCommitter = new AutoCommitter(commitOptions, (c) => { commits.Add(c); });

            // Act
            for (int i = 0; i < ackEvery * 3; i++)
            {
                var receivedPackage = ModelFactory.ConvertToReceivedPackage(new TransportPackage(typeof(object), "somekey", new object()));
                await autoCommitter.Publish(receivedPackage);
            }
            
            // Assert
            commits.Count.Should().Be(3);
            commits.All(x => x.Count == ackEvery).Should().BeTrue();
        }
        
        [Fact]
        public async Task Publish_CommitInterval_ShouldCommitAfterTimeElapsed()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = null,
                CommitInterval = 2000
            };


            var mre = new ManualResetEvent(false);
            var commitTimes = new List<DateTime>();
            var autoCommitter = new AutoCommitter(commitOptions, c=>
            {
                commitTimes.Add(DateTime.UtcNow);
                mre.Set();
            });

            // Act
            var start = DateTime.UtcNow;
            var receivedPackage = ModelFactory.ConvertToReceivedPackage(new TransportPackage(typeof(object), "somekey", new object()));
            await autoCommitter.Publish(receivedPackage);
            mre.WaitOne(3000);

            // Assert
            commitTimes.Count().Should().Be(1);
            var timeItTook = commitTimes[0] - start;
            // build agent doesn't can be a bit wonky, so anywhere between 1.8-3 works
            timeItTook.Should().BeLessThan(TimeSpan.FromSeconds(3));
            timeItTook.Should().BeGreaterOrEqualTo(TimeSpan.FromSeconds(1.8)); // depending on some things it might be slightly less than 2 secs
        }
        
        [Fact]
        public async Task Publish_AutoCommitDisabled_ShouldNotCommit()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = false,
                CommitEvery = 1,
                CommitInterval = null
            };


            var commits = new List<ICollection<TopicPartitionOffset>>();
            var autoCommitter = new AutoCommitter(commitOptions, c => commits.Add(c));

            // Act
            for (int i = 0; i < 12; i++)
            {
                await autoCommitter.Publish(new TransportPackage(typeof(object), "somekey", new object()));
            }

            // Assert
            commits.Count.Should().Be(0);
        }
        
        [Fact]
        public void HandleCommitted_ShouldNotRaiseCommitEvent()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = false
            };


            var commits = new List<ICollection<TopicPartitionOffset>>();
            var modifier = new AutoCommitter(commitOptions, c=> commits.Add(c));

            // Act
            var noError = new Error(ErrorCode.NoError);
            var committedOffsets = new CommittedOffsets(new List<TopicPartitionOffsetError>()
            {
                new TopicPartitionOffsetError(new TopicPartitionOffset("sometopic", new Partition(1), new Offset(123)),
                    noError)
            }, noError);
            modifier.HandleCommitted(new CommittedEventArgs(committedOffsets));
            

            // Assert
            commits.Count.Should().Be(0);
        }
        
        [Fact]
        public void PackageAvailable_CommitterCloses_ShouldCommitOnlyCompleted()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = null,
                CommitInterval = null
            };


            var commits = new List<ICollection<TopicPartitionOffset>>();
            var autoCommitter = new AutoCommitter(commitOptions, c=> commits.Add(c));

            var newPackageCallback = new List<(TransportPackage, TaskCompletionSource<object>)>();
            var counter = 0;
            autoCommitter.OnPackageAvailable = package =>
            {
                var tcs = new TaskCompletionSource<object>();
                newPackageCallback.Add((package, tcs));
                counter++;
                if (counter == 2) tcs.SetResult(new object());
                return tcs.Task;
            };

            var package1 = ModelFactory.ConvertToReceivedPackage(new TransportPackage<object>("key1", new object()));
            var package2 = ModelFactory.ConvertToReceivedPackage(new TransportPackage<object>("key2", new object()));
            var package3 = ModelFactory.ConvertToReceivedPackage(new TransportPackage<object>("key3", new object()));
            
            // Act
            autoCommitter.Publish(package1);
            autoCommitter.Publish(package2);
            autoCommitter.Publish(package3);
            autoCommitter.Close();


            // Assert
            commits.Count.Should().Be(1);
            commits[0].Count.Should().Be(1);
        }
        
        [Fact]
        public async Task CommitCallback_ExceptionOccurs_ShouldNotDeadlock()
        {
            // Arrange
            var commitOptions = new CommitOptions()
            {
                AutoCommitEnabled = true,
                CommitEvery = 1,
                CommitInterval = null
            };
            
            long counter = 0;
            var modifier = new AutoCommitter(commitOptions, c=>
            {
                var val = Interlocked.Increment(ref counter);
                if (val == 1) throw new Exception("whatever");
            });

            var package1Source = new TransportPackage<object>("key1", new object());
            var package2Source = new TransportPackage<object>("key2", new object());
            var package1Received = ModelFactory.ConvertToReceivedPackage(package1Source);
            var package2Received = ModelFactory.ConvertToReceivedPackage(package2Source);

            
            // Act
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await Task.Run(async () =>
            {
                Func<Task> action = () => modifier.Publish(package1Received);
                action.Should().Throw<Exception>().Which.Message.Should().Be("whatever");

                var counterVal = Interlocked.Read(ref counter);
                counterVal.Should().Be(1);
                
                await modifier.Publish(package2Received);
                counterVal = Interlocked.Read(ref counter);
                counterVal.Should().Be(2);
                
            }, cts.Token);

            // Assert
            cts.IsCancellationRequested.Should().BeFalse();
        }
    }
}