using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Collections.Generic;
using MapReduce.Master.Models;

namespace MapReduce.Master
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var fileArray = Directory.GetFiles("test_inputs");

            MasterSettings settings = new()
            {
                InputFilePaths = new List<string>(fileArray),
                IpAddress = "localhost",
                Port = 5000,
                ReduceTaskCount = 6
            };
            Helpers.Master master = new(settings);

            CancellationTokenSource cancelToken = new();

            Task masterTask = master.StartAsync(cancelToken.Token);

            Console.WriteLine("Press any key to stop master.");
            Console.ReadKey();

            cancelToken.Cancel();
            Console.WriteLine("Killing master...");

            try
            {
                await Task.WhenAll(masterTask).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
        }
    }
}
