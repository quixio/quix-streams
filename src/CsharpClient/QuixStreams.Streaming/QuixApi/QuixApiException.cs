using System;
using System.Net;

namespace QuixStreams.Streaming.QuixApi
{
    /// <summary>
    /// API exception converted to a c# exception
    /// </summary>
    public class QuixApiException : Exception
    {

        /// <summary>
        /// Initializes a new instance of <see cref="QuixApiException"/>
        /// </summary>
        /// <param name="endpoint">Endpoint used to access Quix Api</param>
        /// <param name="msg">Error message</param>
        /// <param name="cid">Correlation Id</param>
        /// <param name="httpStatusCode">Http error code</param>
        public QuixApiException(string endpoint, string msg, string cid, HttpStatusCode httpStatusCode) : base(ConvertToMessage(endpoint, msg, cid, httpStatusCode))
        {
        }

        private static string ConvertToMessage(string endpoint, string msg, string cid, HttpStatusCode httpStatusCode)
        {
            if (!string.IsNullOrWhiteSpace(cid))
            {
                return $"Request failed ({(int)httpStatusCode}) to {endpoint} with message: {msg}{(msg.EndsWith(".") ? "" : ".")} If you need help, contact us with Correlation Id {cid}.";
            }

            return $"Request failed ({(int)httpStatusCode}) to {endpoint} with message: {msg}.";
        }
    }
}