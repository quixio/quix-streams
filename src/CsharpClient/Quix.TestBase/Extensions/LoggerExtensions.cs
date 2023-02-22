using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Quix.TestBase.Extensions
{
    public static class LoggerExtensions
    {
        public static List<LoggedMessages> GetLoggedMessages(this ILogger logger)
        {
            return logger.ReceivedCalls().Select(x =>
                                                 {
                                                     var args = x.GetArguments();
                                                     return new LoggedMessages
                                                            {
                                                                LogLevel = (LogLevel) args[0],
                                                                Message = args[2].ToString(),
                                                                Exception = (Exception) args[3]
                                                            };
                                                 }).ToList();
        }
    }

    public class LoggedMessages
    {
        public LogLevel LogLevel { get; set; }

        public string Message { get; set; }

        public Exception Exception { get; set; }
    }
}