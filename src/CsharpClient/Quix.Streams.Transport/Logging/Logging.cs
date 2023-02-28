using System;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Quix.Streams
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

}