using System.Linq;
using Microsoft.Extensions.Logging;
using Quix.Streams.Process.Models;

namespace Quix.Streams.Process.Samples
{
    /// <summary>
    /// Simple writer component that writes tdata messages through the console
    /// </summary>
    public class ConsoleStreamWriter : StreamComponent
    {
        private readonly ILogger logger = Quix.Streams.Logging.CreateLogger<ConsoleStreamWriter>();

        public ConsoleStreamWriter()
        {
            InitializeStreaming();
        }

        private void InitializeStreaming()
        {
            this.Input.Subscribe<TimeseriesDataRaw>(OnTimeseriesDataReceived);
            this.OnStreamProcessAssigned = OnStreamProcessAssignedHandler;
        }

        private void OnStreamProcessAssignedHandler()
        {
            logger.LogInformation("Stream started. StreamId = {0}", this.StreamProcess.StreamId);
        }

        private void OnTimeseriesDataReceived(TimeseriesDataRaw tdata)
        {
            logger.LogInformation("Stream data received. Value = {0}", tdata.NumericValues.First().Value[0]);
        }
    }
}
