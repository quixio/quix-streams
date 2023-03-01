namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Extensions methods to link stream pipelinees together
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
        /// Links two stream pipelinees
        /// </summary>
        /// <param name="source">Source stream pipeline where to take messages from</param>
        /// <param name="target">Target stream pipeline where to send messages to</param>
        /// <returns>Same stream pipeline that used this method, allowing to chain several links in the same line of code</returns>
        public static IStreamPipeline LinkTo(this IStreamPipeline source, IStreamPipeline target)
        {
            source.Subscribe((streamPipeline, package) => target.Send(package));

            return source;
        }

    }
}