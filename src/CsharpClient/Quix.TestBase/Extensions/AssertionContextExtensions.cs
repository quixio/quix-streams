using System;
using FluentAssertions.Equivalency;
using FluentAssertions.Execution;

namespace Quix.TestBase.Extensions
{
    public static class AssertionContextExtensions
    {
        public static void ApproximatelyDateTime(this IAssertionContext<DateTime> ctx, TimeSpan precision)
        {
            var diff = TimeSpan.FromTicks(Math.Abs(ctx.Subject.Ticks - ctx.Expectation.Ticks));
            var withinPrecision = diff < precision;

            Execute.Assertion
                   .BecauseOf(ctx.Because, ctx.BecauseArgs)
                   .ForCondition(withinPrecision)
                   .FailWith("Expected {context:dateTime} to be {0} +/- {1:g} {reason}, but found {2:G}", ctx.Subject,
                             precision, ctx.Expectation);
        }
    }
}