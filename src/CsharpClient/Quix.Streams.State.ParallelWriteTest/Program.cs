using Quix.Streams.State.Storage;
using Quix.Streams.State.Storage.FileStorage.LocalFileStorage;

namespace Quix.Streams.State.ParallelWriteTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Parallel write test");

            var storage = new LocalFileStorage();
            storage.Clear();

            Random rnd = new Random();
            Byte[] data = new byte[] {0, 1, 2, 3};
            storage.Set("GLOBALKEY", data);
            Thread.Sleep(1000);

            int counter = 0;

            var threads = new List<System.Threading.Thread>();
            for (var i = 0; i < 15; ++i)
            {
                var thread = new Thread(() =>
                {
                    Console.WriteLine("STARTING THREAD "+i);
                    
                    Task.Run(
                        (async () =>
                        {
                            try
                            {

                                for (var j = 0; j < 20; ++j)
                                {
                                    Random rnd = new Random();
                                    Byte[] data = new Byte[326 * 1000];
                                    rnd.NextBytes(data);

                                    await storage.SetAsync("GLOBALKEY", data);
                                    await storage.GetBinaryAsync("GLOBALKEY");

                                    Interlocked.Increment(ref counter);
                                }
                            }
                            catch
                            {
                                Console.WriteLine("Exception");
                                throw;
                            }
                        })
                    ).Wait();

                    Console.WriteLine("ENDING THREAD "+i);
                    
                });

                threads.Add(thread);
                
            }

            Console.WriteLine("Starting parallel writes");

            foreach (var thread in threads)
            {
                thread.Start();
            }

            Console.WriteLine("Waiting for parallel writes to complete");

            foreach (var thread in threads)
            {
                thread.Join();
            }

            Console.WriteLine("successfully "+counter+" times read and written in parallel");
            Console.WriteLine("DONE");

        }
    }
}