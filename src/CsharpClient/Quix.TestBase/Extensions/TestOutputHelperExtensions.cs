using System;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit.Abstractions;

namespace Quix.TestBase.Extensions
{
    public static class TestOutputHelperExtensions
    {
        public static ILogger<T> ConvertToLogger<T>(this ITestOutputHelper helper)
        {
            return new TestLogger<T>(helper);
        }

        public static ILoggerFactory CreateLoggerFactory(this ITestOutputHelper helper)
        {
            var factory = Substitute.For<ILoggerFactory>();
            factory.CreateLogger(Arg.Any<string>()).Returns(ci =>
            {
                var name = ci.Arg<string>();
                return new TestLogger(helper, name);
            });
            return factory;
        }
    }

    public class TestLogger<T> : ILogger<T>
    {
        private readonly string classDisplayName;
        private readonly ITestOutputHelper helper;

        public TestLogger(ITestOutputHelper helper)
        {
            this.classDisplayName = typeof(T).Name;
            this.helper = helper;
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception exception,
            Func<TState, Exception, string> formatter)
        {
            try
            {
                this.helper.WriteLine($"{DateTime.UtcNow:G} - {logLevel} - {this.classDisplayName} {formatter(state, exception)}");
                if (exception != null)
                {
                    if (exception.Message != null) this.helper.WriteLine(exception.Message);
                    if (exception.StackTrace != null) this.helper.WriteLine(exception.StackTrace);
                }
            }
            catch (System.InvalidOperationException)
            {
                // I think this might happen with async tasks, but stop failing agent tests for this
            }
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }
    }

    public class TestLogger : ILogger
    {
        private readonly string categoryName;
        private readonly ITestOutputHelper helper;

        public TestLogger(ITestOutputHelper helper, string categoryName)
        {
            this.categoryName = categoryName;
            this.helper = helper;
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception exception,
            Func<TState, Exception, string> formatter)
        {
            try
            {
                this.helper.WriteLine($"{DateTime.UtcNow:G} - {logLevel} - {this.categoryName} {formatter(state, exception)}");
                if (exception != null)
                {
                    if (exception.Message != null) this.helper.WriteLine(exception.Message);
                    if (exception.StackTrace != null) this.helper.WriteLine(exception.StackTrace);
                }
            }
            catch (System.InvalidOperationException)
            {
                // I think this might happen with async tasks, but stop failing agent tests for this
            }
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }
    }
}