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
            Console.WriteLine("Hello World! Here are workers!");
            // await DoWordCountAsync();
            await DoInvertedIndexAsync();
        }

        private static async Task DoInvertedIndexAsync()
        {
            InvertedIndex invertedIndex = new();
            DefaultPartitioner<string, List<object>> defaultPartitioner = new();

            RpcClientFactory rpcClientFactory = new(new()
            {
                Address = "http://localhost:5000"
            });

            const int numWorkers = 12;

            List<Helpers.Worker<string, List<object>, Dictionary<string, List<int>>>> workers = new();
            List<Task> tasks = new();

            try
            {
                int i;
                for (i = 0; i < numWorkers; i++)
                {
                    Helpers.Worker<string, List<object>, Dictionary<string, List<int>>> worker = new(
                        settings: new()
                        {
                            WorkerUuid = Guid.NewGuid().ToString(),
                            MappedOutputDirectory = "mapped",
                            ReducedOutputDirectory = "reduced",
                        },
                        rpcClientFactory: rpcClientFactory,
                        mappingPhase: invertedIndex,
                        reducingPhase: invertedIndex,
                        partitioningPhase: defaultPartitioner
                    );

                    CancellationTokenSource cancelToken = new();

                    Task task = worker.RunAsync(cancelToken.Token);

                    workers.Add(worker);
                    tasks.Add(task);
                }

                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (TaskCanceledException) { }
            }
            finally
            {
                foreach (var worker in workers)
                {
                    worker.Dispose();
                }
            }
        }

        private static async Task DoWordCountAsync()
        {
            WordCount wordCount = new();
            DefaultPartitioner<string, int> defaultPartitioner = new();

            RpcClientFactory rpcClientFactory = new(new()
            {
                Address = "http://localhost:5000"
            });

            const int numWorkers = 12;

            List<Helpers.Worker<string, int, int>> workers = new();
            List<Task> tasks = new();

            try
            {
                int i;
                for (i = 0; i < numWorkers; i++)
                {
                    Helpers.Worker<string, int, int> worker = new(
                        settings: new()
                        {
                            WorkerUuid = Guid.NewGuid().ToString(),
                            MappedOutputDirectory = "mapped",
                            ReducedOutputDirectory = "reduced",
                        },
                        rpcClientFactory: rpcClientFactory,
                        mappingPhase: wordCount,
                        reducingPhase: wordCount,
                        partitioningPhase: defaultPartitioner
                    );

                    CancellationTokenSource cancelToken = new();

                    Task task = worker.RunAsync(cancelToken.Token);

                    workers.Add(worker);
                    tasks.Add(task);
                }

                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (TaskCanceledException) { }
            }
            finally
            {
                foreach (var worker in workers)
                {
                    worker.Dispose();
                }
            }
        }
    }
}
