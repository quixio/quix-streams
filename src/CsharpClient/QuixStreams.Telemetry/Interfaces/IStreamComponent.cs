
namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Stands for one Component in a Stream pipeline chain.
    /// Each component is connected to one another using Input and Output connection points
    /// </summary>
    public interface IStreamComponent
    {
        /// <summary>
        /// Input connection point of the component. Commonly used inside a component implementation to read messages.
        /// </summary>
        IIOComponentConnection Input { get; }

        /// <summary>
        /// Output connection point of the component. Commonly used inside a component implementation to write messages.
        /// </summary>
        IIOComponentConnection Output { get; }

        /// <summary>
        /// Stream pipeline that owns the component
        /// </summary>
        IStreamPipeline StreamPipeline { get; set; }
    }
}