﻿using System;
 using System.Linq;
 using System.Threading;
 using System.Threading.Tasks;
 using QuixStreams.Telemetry.Models;

 namespace QuixStreams.Telemetry.Samples
{
    /// <summary>
    /// Simple reader component that generates random timeseries data messages and sends them to the output.
    /// </summary>
    public class RandomDataProducer : StreamComponent
    {

        public RandomDataProducer()
        {
        }

        public void Start(CancellationToken ct)
        {
            try
            {
                var offset = 0;
                while (!ct.IsCancellationRequested)
                {
                    var tdata = GenerateTimeseriesData(offset);

                    this.Output.Send(tdata);

                    Task.Delay(1000, ct).Wait(ct);
                    offset = +10;
                }
            }
            catch (OperationCanceledException)
            {
                if (!ct.IsCancellationRequested) throw;
            }
        }

        private static TimeseriesDataRaw GenerateTimeseriesData(int offset)
        {
            return new TimeseriesDataRaw
            {
                Epoch = 1000000,
                Timestamps = Enumerable.Range(offset, 10).Select(s => (long)s).ToArray(),
                NumericValues = Enumerable.Range(0, 2).ToDictionary(k => "p" + k, s => Enumerable.Range(offset, 10).Select(s => new double?(s * 10.0)).ToArray())
            };
        }

    }
}
