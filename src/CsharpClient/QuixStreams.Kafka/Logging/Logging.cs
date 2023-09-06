using System;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using ILogger = Microsoft.Extensions.Logging.ILogger;

// ReSharper disable once CheckNamespace
namespace QuixStreams
{
    public static class Logging
    {
        static Logging()
        {
            UpdateFactory(LogLevel.Information);
        }
        
        // ReSharper disable once FieldCanBeMadeReadOnly.Global
        // ReSharper disable once MemberCanBePrivate.Global
        public static ILoggerFactory Factory = null;
        
        /// <summary>
        /// Creates a logger factory that prefixes log messages with a specified string.
        /// </summary>
        /// <param name="prefix">The prefix to be added to the log messages.</param>
        /// <returns>A new instance of the <see cref="PrefixedLoggerFactory"/> class that prefixes log messages with the specified string.</returns>
        public static ILoggerFactory CreatePrefixedFactory(string prefix) => new PrefixedLoggerFactory(Factory, prefix);

        /// <summary>
        /// Updates the factory with the specified log level.
        /// Allows for easy update of log level without modifying the factory fundamentally
        /// </summary>
        /// <param name="logLevel"></param>
        public static void UpdateFactory(LogLevel logLevel)
        {
            Factory = LoggerFactory.Create(c =>
            {
                c.ClearProviders();
                c.SetMinimumLevel(logLevel);

                var builder = new LoggerConfiguration()
                        .Enrich.FromLogContext()
                        .Enrich.WithThreadId()
                        .WriteTo.Console(theme: AnsiConsoleTheme.Literate, applyThemeToRedirectedOutput: true, outputTemplate: "[{Timestamp:yy-MM-dd HH:mm:ss.fff} ({ThreadId}) {Level:u3}] {Message:lj}{NewLine}{Exception}");
                switch (logLevel)
                {
                    case LogLevel.Trace:
                        builder.MinimumLevel.Verbose();
                        break;
                    case LogLevel.Debug:
                        builder.MinimumLevel.Debug();
                        break;
                    case LogLevel.Information:
                        builder.MinimumLevel.Information();
                        break;
                    case LogLevel.Warning:
                        builder.MinimumLevel.Warning();
                        break;
                    case LogLevel.Error:
                        builder.MinimumLevel.Error();
                        break;
                    case LogLevel.Critical:
                        builder.MinimumLevel.Fatal();
                        break;
                    case LogLevel.None:
                        builder.MinimumLevel.Fatal(); // there is no None, closest to it is this
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
                }

                var logger = builder.CreateLogger();
                c.AddSerilog(logger, dispose:true);
                
                SerilogWindowsConsole.EnableVirtualTerminalProcessing();
            });
            
            if (logLevel <= LogLevel.Debug)
            {
                CreateLogger(typeof(Logging)).LogDebug($"Quix Streams logging factory set to {logLevel} log level");
            }
        }

        /// <summary>
        /// Shortcut to <see cref="Factory.CreateLogger"/>
        /// </summary>
        /// <typeparam name="T">The type to create the logger for</typeparam>
        /// <returns>Logger created for the type</returns>
        public static ILogger<T> CreateLogger<T>()
        {
            return Factory.CreateLogger<T>();
        }
        
        /// <summary>
        /// Shortcut to <see cref="Factory.CreateLogger"/>
        /// </summary>
        /// <param name="type">The type to create the logger for</param>
        /// <returns>Logger created for the type</returns>
        public static ILogger CreateLogger(Type type)
        {
            return Factory.CreateLogger(type);
        }
    }

    /// <summary>
    /// A logger factory that can be used to prefix log messages with a specified string.
    /// </summary>
    public class PrefixedLoggerFactory : ILoggerFactory
    {
        private readonly ILoggerFactory innerFactory;
        private readonly string prefix;

        /// <summary>
        /// Initializes a new instance of the <see cref="PrefixedLoggerFactory"/> class.
        /// </summary>
        /// <param name="innerFactory">The inner logger factory.</param>
        /// <param name="prefix">The prefix to be added to the log messages.</param>
        public PrefixedLoggerFactory(ILoggerFactory innerFactory, string prefix)
        {
            this.innerFactory = innerFactory;
            this.prefix = prefix;
        }

        /// <inheritdoc/>
        public void AddProvider(ILoggerProvider provider)
        {
            innerFactory.AddProvider(provider);
        }

        /// <inheritdoc/>
        public ILogger CreateLogger(string categoryName)
        {
            ILogger innerLogger = innerFactory.CreateLogger(categoryName);
            return new PrefixedLogger(innerLogger, prefix);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            innerFactory.Dispose();
        }

        /// <summary>
        /// A logger that prefixes log messages with a specified string.
        /// </summary>
        private class PrefixedLogger : ILogger
        {
            private readonly ILogger innerLogger;
            private readonly string _prefix;

            /// <summary>
            /// Initializes a new instance of the <see cref="PrefixedLogger"/> class.
            /// </summary>
            /// <param name="innerLogger">The inner logger.</param>
            /// <param name="prefix">The prefix to be added to the log messages.</param>
            public PrefixedLogger(ILogger innerLogger, string prefix)
            {
                this.innerLogger = innerLogger;
                _prefix = prefix;
            }

            /// <inheritdoc/>
            public IDisposable BeginScope<TState>(TState state)
            {
                return innerLogger.BeginScope(state);
            }

            /// <inheritdoc/>
            public bool IsEnabled(LogLevel logLevel)
            {
                return innerLogger.IsEnabled(logLevel);
            }

            /// <inheritdoc/>
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                var prefixedMessage = $"{_prefix} | {formatter(state, exception)}";
                innerLogger.Log(logLevel, eventId, state, exception, (s, e) => prefixedMessage);
            }
        }
    }



}