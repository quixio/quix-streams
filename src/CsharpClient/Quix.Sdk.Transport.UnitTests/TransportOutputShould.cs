using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.UnitTests.Helpers;
using Xunit;

namespace Quix.Sdk.Transport.UnitTests
{
    public class TransportOutputShould
    {
        [Fact]
        public void Commit_WithTransport_ShouldAckOutput()
        {
            // Arrange
            var output = Substitute.For<IOutput, ICanCommit>();
            TransportContext[] committed = null;
            ((ICanCommit)output).Commit(Arg.Do<TransportContext[]>(x => committed = x));
            var transportOutput = new TransportOutput(output);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            var transportContext2 = new TransportContext()
            {
                {"abcdef", 2}
            };
            
            // Act
            transportOutput.Commit(new [] {transportContext, transportContext2});

            // Assert
            committed.Should().BeEquivalentTo(transportContext, transportContext2);
        }
        
        [Fact]
        public void Setup_OutputRaisesEnoughForAutoCommitToKickIn_ShouldCommitIntoOutput()
        {
            // Arrange
            var output = new Passthrough();
            OnCommittedEventArgs committed = null;
            output.OnCommitted += (sender, args) => committed = args;
            var transportOutput = new TransportOutput(output, o=> o.CommitOptions.CommitEvery = 1);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };

            var package = PackageFactory.CreatePackage(new {TestMessage = "YAY"}, transportContext);
            
            // Act
            output.OnNewPackage.Invoke(package);

            // Assert
            committed.Should().NotBeNull();
            committed.State.Should().BeEquivalentTo(new[] {transportContext}); // PassThrough sends back the transport context as state
        }
        
        [Fact]
        public void Setup_OutputRaisesRevokingEvent_ShouldAlsoRaiseRevokingEvent()
        {
            // Arrange
            var output = Substitute.For<IOutput, IRevocationPublisher>();
            var transportOutput = new TransportOutput(output);
            var revokingInvokes = new List<object>();
            transportOutput.OnRevoking += (sender, args) =>
            {
                revokingInvokes.Add(args);
            };
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var expectedArgs = new OnRevokingEventArgs();
            ((IRevocationPublisher)output).OnRevoking += Raise.EventWith(expectedArgs);

            // Assert
            revokingInvokes.Should().BeEquivalentTo(new[] {expectedArgs});
        }
        
        [Fact]
        public void Setup_OutputRaisesRevokedEvent_ShouldAlsoRaiseRevokedEvent()
        {
            // Arrange
            var output = Substitute.For<IOutput, IRevocationPublisher>();
            var transportOutput = new TransportOutput(output);
            var revokedInvokes = new List<object>();
            transportOutput.OnRevoked += (sender, args) =>
            {
                revokedInvokes.Add(args);
            };
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var expectedArgs = new OnRevokedEventArgs();
            ((IRevocationPublisher)output).OnRevoked += Raise.EventWith(expectedArgs);

            // Assert
            revokedInvokes.Should().BeEquivalentTo(new[] {expectedArgs});
        }
        
        [Fact]
        public void Setup_OutputSupportsRevocationPublisher_FilterRevokedContextsShouldBeSameAsOutputs()
        {
            // As of writing this test, No other modifier within TransportOutput supports IRevocationFilter,
            // meaning if in future this changes, this test will have to be updated to whatever the new logic is.
            // For now it is assumed that the provided output might implement IRevocationPublisher
            // Arrange
            var output = Substitute.For<IOutput, IRevocationPublisher>();
            var transportOutput = new TransportOutput(output);
            var expectedResult = new [] {new TransportContext()
            {
                {"abcde", 1}
            }};
            ((IRevocationPublisher) output).FilterRevokedContexts(Arg.Any<object>(), Arg.Any<IEnumerable<TransportContext>>()).ReturnsForAnyArgs(expectedResult);

            // Act
            var result = transportOutput.FilterRevokedContexts(null, null); // se mock above, nulls don't matter

            // Assert
            result.Should().BeEquivalentTo(expectedResult);
        }
        
        [Fact]
        public void Setup_OutputRaisesCommitted_ShouldAlsoRaiseCommitted()
        {
            // Arrange
            var output = Substitute.For<IOutput, ICanCommit>();
            var committed = new List<OnCommittedEventArgs>();
            var transportOutput = new TransportOutput(output, o=> o.CommitOptions.CommitEvery = 1);
            transportOutput.OnCommitted += (sender, args) => committed.Add(args);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var eArgs = new OnCommittedEventArgs(new object());
            ((ICanCommit) output).OnCommitted += Raise.EventWith(eArgs);

            // Assert
            var expected = new List<OnCommittedEventArgs> {eArgs};
            committed.Should().BeEquivalentTo(expected);
        }
        
                
        [Fact]
        public void Setup_OutputRaisesCommitting_ShouldAlsoRaiseCommitting()
        {
            // Arrange
            var output = Substitute.For<IOutput, ICanCommit>();
            var comittingArgs = new List<OnCommittingEventArgs>();
            var transportOutput = new TransportOutput(output, o=> o.CommitOptions.CommitEvery = 1);
            transportOutput.OnCommitting += (sender, args) => comittingArgs.Add(args);
            
            var transportContext = new TransportContext()
            {
                {"abcde", 1}
            };
            
            // Act
            var eArgs = new OnCommittingEventArgs(new object());
            ((ICanCommit) output).OnCommitting += Raise.EventWith(eArgs);

            // Assert
            var expected = new List<OnCommittingEventArgs> {eArgs};
            comittingArgs.Should().BeEquivalentTo(expected);
        }
    }
}
