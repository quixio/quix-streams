using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry.Samples
{
    /// <summary>
    /// Simple writer component that writes tdata messages through the console
    /// </summary>
    public class ConsoleStreamWriter : StreamComponent
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<ConsoleStreamWriter>();

        public ConsoleStreamWriter()
        {
            InitializeStreaming();
        }

        private void InitializeStreaming()
        {
            this.Input.Subscribe<TimeseriesDataRaw>(OnTimeseriesDataReceived);
            this.OnStreamPipelineAssigned = OnStreamPipelineAssignedHandler;
        }

        private void OnStreamPipelineAssignedHandler()
        {
            logger.LogInformation("Stream started. StreamId = {0}", this.StreamPipeline.StreamId);
        }

        private void OnTimeseriesDataReceived(TimeseriesDataRaw tdata)
        {
            logger.LogInformation("Stream data received. Value = {0}", tdata.NumericValues.First().Value[0]);
        }
    }
}
