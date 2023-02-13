using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Mono.Unix;
using Mono.Unix.Native;
using Quix.Sdk.Streaming.Raw;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Helper class to handle default streaming behaviors and handle automatic resource cleanup on shutdown
    /// </summary>
    public static class App
    {
        private static readonly ConcurrentDictionary<IInputTopic, bool> inputTopics = new ConcurrentDictionary<IInputTopic, bool>();
        private static readonly ConcurrentDictionary<IRawInputTopic, bool> rawInputTopics = new ConcurrentDictionary<IRawInputTopic, bool>();
        private static readonly ConcurrentDictionary<IOutputTopic, bool> outputTopics = new ConcurrentDictionary<IOutputTopic, bool>();
        private static readonly ConcurrentDictionary<IRawOutputTopic, bool> rawOutputTopics = new ConcurrentDictionary<IRawOutputTopic, bool>();
        
        
        [DllImport("Kernel32")]
        private static extern bool SetConsoleCtrlHandler(HandlerRoutine handler, bool add);

        private delegate bool HandlerRoutine(CtrlType sig);

        private enum CtrlType {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT = 1,
            CTRL_CLOSE_EVENT = 2,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT = 6
        }

        /// <summary>
        /// Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown
        /// It also ensures input topics defined at the time of invocation are opened for read.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token to abort. Use when you wish to manually stop streaming for other reason that shutdown.</param>
        /// <param name="beforeShutdown">The callback to invoke before shutting down</param>
        public static void Run(CancellationToken cancellationToken = default, Action beforeShutdown = null)
        {
            var logger = Sdk.Logging.CreateLogger<object>();
            var waitForProcessShutdownStart = new ManualResetEventSlim();
            var waitForMainExit = new ManualResetEventSlim();
            Action actualBeforeShutdown = () =>
            {
                if (beforeShutdown == null) return;
                logger.LogDebug("Invoking user provided shutdown callback");
                try
                {
                    beforeShutdown();
                    logger.LogDebug("Invoked user provided shutdown callback");
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Exception while invoking user provided shutdown callback");                    
                }
            };

            switch (Environment.OSVersion.Platform)
            {
                case PlatformID.Win32NT:
                    var handler = new HandlerRoutine(type =>
                    {
                        logger.LogDebug("Termination signal: {0}", type);
                        waitForProcessShutdownStart.Set();
                        return false;
                    });
                    SetConsoleCtrlHandler(handler, true);
                    
                    AppDomain.CurrentDomain.ProcessExit += (sender, e) =>   
                    {
                        // Don't unwind until main exits
                        waitForMainExit.Wait();
                    };                    
                    break;
                case PlatformID.MacOSX:
                case PlatformID.Unix:
                    Task.Run(() =>
                    {
                        var signals = new []
                        {
                            new UnixSignal(Signum.SIGTERM),
                            new UnixSignal(Signum.SIGQUIT),
                            new UnixSignal(Signum.SIGINT)
                        };
                        var signalIndex = UnixSignal.WaitAny(signals);
                        var signal = signals[signalIndex];
                        logger.LogDebug("Termination signal: {0}", signal.Signum);
                        waitForProcessShutdownStart.Set();
                    }, cancellationToken);
                    AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
                    {
                        // Don't unwind until main exits
                        waitForMainExit.Wait();
                    };
                    break;
                case PlatformID.Win32S:
                case PlatformID.Win32Windows:
                case PlatformID.WinCE:
                case PlatformID.Xbox:
                    Console.CancelKeyPress += (sender, args) =>
                    {
                        waitForProcessShutdownStart.Set();
                    };
                    AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
                    {
                        // We got a SIGTERM, signal that graceful shutdown has started
                        waitForProcessShutdownStart.Set();
                        
                        // Don't unwind until main exits
                        waitForMainExit.Wait();
                    };                    
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (cancellationToken != default)
            {
                cancellationToken.Register(() =>
                {
                    waitForProcessShutdownStart.Set();

                    // Don't unwind until main exits
                    waitForMainExit.Wait();
                });
            }

            var opened = 0;
            
            // Init
            foreach (var inputTopic in inputTopics)
            {
                inputTopic.Key.StartReading();
                opened++;
            }
            
            foreach (var inputTopic in rawInputTopics)
            {
                inputTopic.Key.StartReading();
                opened++;
            }
            if (opened > 0) logger.LogDebug("Opened {0} topics for reading", opened);
            else logger.LogDebug("There were no topics top open for reading");

            // Wait for shutdown to start
            waitForProcessShutdownStart.Wait();

            logger.LogDebug($"Waiting for graceful shutdown");
            var sw = Stopwatch.StartNew();
            actualBeforeShutdown();
            foreach (var disposable in inputTopics)
            {
                try
                {
                    disposable.Key.Dispose();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Exception while disposing {0}", disposable.GetType().FullName);
                }
            }
            
            foreach (var disposable in rawInputTopics)
            {
                try
                {
                    disposable.Key.Dispose();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Exception while disposing {0}", disposable.GetType().FullName);
                }
            }
            
            foreach (var disposable in outputTopics)
            {
                try
                {
                    disposable.Key.Dispose();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Exception while disposing {0}", disposable.GetType().FullName);
                }
            }

            foreach (var disposable in rawOutputTopics)
            {
                try
                {
                    disposable.Key.Dispose();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Exception while disposing {0}", disposable.GetType().FullName);
                }
            }
            
            rawInputTopics.Clear();
            inputTopics.Clear();
            outputTopics.Clear();
            rawOutputTopics.Clear();

            logger.LogDebug("Graceful shutdown completed in {0}", sw.Elapsed);

            // Now we're done with main, tell the shutdown handler
            waitForMainExit.Set();
        }

        internal static void Register(InputTopic inputTopic)
        {
            if (inputTopics.TryAdd(inputTopic, false)) inputTopic.OnDisposed += (s, e) => inputTopics.TryRemove(inputTopic, out _);
        }
        

        internal static void Register(RawInputTopic rawInputTopic)
        {
            if (rawInputTopics.TryAdd(rawInputTopic, false)) rawInputTopic.OnDisposed += (s, e) => rawInputTopics.TryRemove(rawInputTopic, out _);
        }

        internal static void Register(OutputTopic rawOutputTopic)
        {
            if (outputTopics.TryAdd(rawOutputTopic, false)) rawOutputTopic.OnDisposed += (s, e) => outputTopics.TryRemove(rawOutputTopic, out _);
        }

        internal static void Register(RawOutputTopic rawOutputTopic)
        {
            if (rawOutputTopics.TryAdd(rawOutputTopic, false)) rawOutputTopic.OnDisposed += (s, e) => rawOutputTopics.TryRemove(rawOutputTopic, out _);
        }
    }
}