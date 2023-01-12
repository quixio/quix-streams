using Quix.Sdk.Process;
using Quix.Sdk.Process.Models;
using System.Linq;
using System.Threading.Tasks;

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
                .Intercept<ParameterDataRaw>(OnTDataIntercept) // use here any other generic model type
                .Intercept<ParameterDataRaw>(OnTDataIntercept) // use here any other generic model type
                .Intercept<ParameterDataRaw>(OnTDataIntercept);
        }

        public Task OnTDataIntercept(ParameterDataRaw tdata)
        {
            tdata.NumericValues.First().Value[0] = this.num;

            return Output.Send(tdata);
        }
    }
}
