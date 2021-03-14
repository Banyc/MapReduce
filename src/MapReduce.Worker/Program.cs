using System.Collections.Generic;
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
            DefaultPartitioner<string, int> defaultPartitioner = new();

            RpcClientFactory rpcClientFactory = new(new()
            {
                Address = "http://localhost:5000"
            });

            const int numWorkers = 12;

            List<Helpers.Worker<string, int>> workers = new();
            List<Task> tasks = new();

            int i;
            for (i = 0; i < numWorkers; i++)
            {
                Helpers.Worker<string, int> worker = new(
                    settings: new()
                    {
                        WorkerUuid = Guid.NewGuid().ToString(),
                        MappedOutputDirectory = "mapped",
                        ReducedOutputDirectory = "reduced",
                    },
                    rpcClientFactory: rpcClientFactory,
                    mappingPhase: workCount,
                    reducingPhase: workCount,
                    partitioningPhase: defaultPartitioner
                );

                CancellationTokenSource cancelToken = new();

                Task task = worker.StartAsync(cancelToken.Token);

                workers.Add(worker);
                tasks.Add(task);
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
        }
    }
}
