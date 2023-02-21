using System.Linq;
using System.Threading.Tasks;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Process.Samples
{
    /// <summary>
    /// Simply modifier component that reads tdata messages, change one of his values, and writes the message again to the output.
    /// </summary>
    public class SimplyModifier : StreamComponent
    {
        private readonly int num;

        public SimplyModifier(int num)
        {
            this.num = num;

            // Modifiers is just about links input to output and intercept messages
            Input.LinkTo(Output)
                .Intercept<TimeseriesDataRaw>(OnTDataIntercept) // use here any other generic model type
                .Intercept<TimeseriesDataRaw>(OnTDataIntercept) // use here any other generic model type
                .Intercept<TimeseriesDataRaw>(OnTDataIntercept);
        }

        public Task OnTDataIntercept(TimeseriesDataRaw tdata)
        {
            tdata.NumericValues.First().Value[0] = this.num;

            return Output.Send(tdata);
        }
    }
}
