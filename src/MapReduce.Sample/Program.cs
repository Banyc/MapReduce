using MapReduce.Master.Helpers;
using MapReduce.Master.Models;
using MapReduce.Sample.Playbook;

Console.WriteLine("Hello World!");

// build local workers
Task workerTask = WorkerHelper.DoInvertedIndexAsync();

// build master

var fileArray = Directory.GetFiles("test_inputs");

MasterSettings settings = new()
{
    InputFilePaths = new List<string>(fileArray),
    IpAddress = "localhost",
    Port = 5000,
    ReduceTaskCount = 6
};
Master master = new(settings);

CancellationTokenSource cancelToken = new();

var masterTask = master.MapreduceAsync(cancelToken.Token);

// Console.WriteLine("Press any key to stop master.");
// Console.ReadKey();

// cancelToken.Cancel();
// Console.WriteLine("Killing master...");

try
{
    var mapreducedFiles = await masterTask.ConfigureAwait(false);
    Console.WriteLine("[info] Reduced outputs are:");
    foreach (var mapreducedFile in mapreducedFiles)
    {
        Console.WriteLine($"[info] {mapreducedFile.FilePath}");
    }
}
catch (TaskCanceledException) { }
