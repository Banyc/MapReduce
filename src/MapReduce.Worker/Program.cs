using System.Threading;
using System;
using System.Threading.Tasks;
using MapReduce.Worker.Helpers;

namespace MapReduce.Worker
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            WordCount workCount = new();

            RpcClientFactory rpcClientFactory = new(new(){
                Address = "http://localhost:5000"
            });

            using Helpers.Worker<string, int> worker = new(
                settings: new()
                {
                    WorkerUuid = Guid.NewGuid().ToString(),
                    MappedOutputDirectory = "mapped",
                    ReducedOutputDirectory = "reduced",
                },
                rpcClientFactory: rpcClientFactory,
                mappingPhase: workCount,
                reducingPhase: workCount,
                partitioningPhase: new DefaultPartitioner<string, int>()
            );

            CancellationTokenSource cancelToken = new();

            Task task = worker.StartAsync(cancelToken.Token);

            // Console.WriteLine("Press any key to stop worker.");
            // Console.ReadKey();

            // cancelToken.Cancel();
            // Console.WriteLine("Killing worker...");

            try
            {
                await Task.WhenAll(task).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
        }
    }
}
