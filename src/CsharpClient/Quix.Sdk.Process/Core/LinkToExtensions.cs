namespace Quix.Sdk.Process
{
    /// <summary>
    /// Extensions methods to link stream processes together
    /// </summary>
    public static class LinkToExtensions
    {
        /// <summary>
        /// Links two components' connection points
        /// </summary>
        /// <param name="source">Source connection point where to take messages from</param>
        /// <param name="target">Target connection point where to send messages to</param>
        /// <returns>Same connection point that used this method, allowing to chain several subscriptions or intercepts in the same line of code</returns>
        public static IIOComponentConnection LinkTo(this IIOComponentConnection source, IIOComponentConnection target)
        {
            source.Subscribe(package => target.Send(package));

            return source;
        }

        /// <summary>
        /// Links two stream processes
        /// </summary>
        /// <param name="source">Source stream process where to take messages from</param>
        /// <param name="target">Target stream process where to send messages to</param>
        /// <returns>Same stream process that used this method, allowing to chain several links in the same line of code</returns>
        public static IStreamProcess LinkTo(this IStreamProcess source, IStreamProcess target)
        {
            source.Subscribe((process, package) => target.Send(package));

            return source;
        }

    }
}