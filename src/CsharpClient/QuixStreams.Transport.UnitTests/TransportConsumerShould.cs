using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.UnitTests.Helpers;
using Xunit;

namespace QuixStreams.Transport.UnitTests
{
    public class TransportConsumerShould
    {
        [Fact]
        public void Commit_WithTransport_ShouldAckConsumer()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer, ICanCommit>();
            TransportContext[] committed = null;
            ((ICanCommit)consumer).Commit(Arg.Do<TransportContext[]>(x => committed = x));
            var transportConsumer = new TransportConsumer(consumer);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            var transportContext2 = new TransportContext()
            {
                {"abcdef", 2}
            };
            
            // Act
            transportConsumer.Commit(new [] {transportContext, transportContext2});

            // Assert
            committed.Should().BeEquivalentTo(new [] {transportContext, transportContext2});
        }
        
        [Fact]
        public void Setup_ConsumerRaisesEnoughForAutoCommitToKickIn_ShouldCommitIntoConsumer()
        {
            // Arrange
            var consumer = new Passthrough();
            OnCommittedEventArgs committed = null;
            consumer.OnCommitted += (sender, args) => committed = args;
            var transportConsumer = new TransportConsumer(consumer, o=> o.CommitOptions.CommitEvery = 1);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };

            var package = PackageFactory.CreatePackage(new {TestMessage = "YAY"}, transportContext);
            
            // Act
            consumer.OnNewPackage.Invoke(package);

            // Assert
            committed.Should().NotBeNull();
            committed.State.Should().BeEquivalentTo(new[] {transportContext}); // PassThrough sends back the transport context as state
        }
        
        [Fact]
        public void Setup_ConsumerRaisesRevokingEvent_ShouldAlsoRaiseRevokingEvent()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer, IRevocationPublisher>();
            var transportConsumer = new TransportConsumer(consumer);
            var revokingInvokes = new List<object>();
            transportConsumer.OnRevoking += (sender, args) =>
            {
                revokingInvokes.Add(args);
            };
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var expectedArgs = new OnRevokingEventArgs();
            ((IRevocationPublisher)consumer).OnRevoking += Raise.EventWith(expectedArgs);

            // Assert
            revokingInvokes.Should().BeEquivalentTo(new[] {expectedArgs});
        }
        
        [Fact]
        public void Setup_ConsumerRaisesRevokedEvent_ShouldAlsoRaiseRevokedEvent()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer, IRevocationPublisher>();
            var transportConsumer = new TransportConsumer(consumer);
            var revokedInvokes = new List<object>();
            transportConsumer.OnRevoked += (sender, args) =>
            {
                revokedInvokes.Add(args);
            };
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var expectedArgs = new OnRevokedEventArgs();
            ((IRevocationPublisher)consumer).OnRevoked += Raise.EventWith(expectedArgs);

            // Assert
            revokedInvokes.Should().BeEquivalentTo(new[] {expectedArgs});
        }
        
        [Fact]
        public void Setup_ConsumerSupportsRevocationPublisher_FilterRevokedContextsShouldBeSameAsConsumers()
        {
            // As of writing this test, No other modifier within TransportConsumer supports IRevocationFilter,
            // meaning if in future this changes, this test will have to be updated to whatever the new logic is.
            // For now it is assumed that the provided output might implement IRevocationPublisher
            // Arrange
            var consumer = Substitute.For<IConsumer, IRevocationPublisher>();
            var transportConsumer = new TransportConsumer(consumer);
            var expectedResult = new [] {new TransportContext()
            {
                {"abcde", 1}
            }};
            ((IRevocationPublisher) consumer).FilterRevokedContexts(Arg.Any<object>(), Arg.Any<IEnumerable<TransportContext>>()).ReturnsForAnyArgs(expectedResult);

            // Act
            var result = transportConsumer.FilterRevokedContexts(null, null); // se mock above, nulls don't matter

            // Assert
            result.Should().BeEquivalentTo(expectedResult);
        }
        
        [Fact]
        public void Setup_ConsumerRaisesCommitted_ShouldAlsoRaiseCommitted()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer, ICanCommit>();
            var committed = new List<OnCommittedEventArgs>();
            var transportConsumer = new TransportConsumer(consumer, o=> o.CommitOptions.CommitEvery = 1);
            transportConsumer.OnCommitted += (sender, args) => committed.Add(args);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var eArgs = new OnCommittedEventArgs(new object());
            ((ICanCommit) consumer).OnCommitted += Raise.EventWith(eArgs);

            // Assert
            var expected = new List<OnCommittedEventArgs> {eArgs};
            committed.Should().BeEquivalentTo(expected);
        }
        
                
        [Fact]
        public void Setup_ConsumerRaisesCommitting_ShouldAlsoRaiseCommitting()
        {
            // Arrange
            var consumer = Substitute.For<IConsumer, ICanCommit>();
            var comittingArgs = new List<OnCommittingEventArgs>();
            var transportConsumer = new TransportConsumer(consumer, o=> o.CommitOptions.CommitEvery = 1);
            transportConsumer.OnCommitting += (sender, args) => comittingArgs.Add(args);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var eArgs = new OnCommittingEventArgs(new object());
            ((ICanCommit) consumer).OnCommitting += Raise.EventWith(eArgs);

            // Assert
            var expected = new List<OnCommittingEventArgs> {eArgs};
            comittingArgs.Should().BeEquivalentTo(expected);
        }
    }
}
