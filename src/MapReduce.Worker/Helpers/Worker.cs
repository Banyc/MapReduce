using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Grpc.Core;
using MapReduce.Shared;
using MapReduce.Shared.Helpers;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Worker<TKey, TValue>
    {
        private readonly string _workerUuid = Guid.NewGuid().ToString();
        private readonly WorkerInfoDto _workerInfoDto;
        private readonly WorkerSettings _settings;

        private readonly IMapping<TKey, TValue> _mappingPhase;
        private readonly IReducing<TKey, TValue> _reducingPhase;
        private readonly IPartitioning<TKey, TValue> _partitioningPhase;

        public Worker(
            WorkerSettings settings,
            IMapping<TKey, TValue> mappingPhase,
            IReducing<TKey, TValue> reducingPhase,
            IPartitioning<TKey, TValue> partitioningPhase)
        {
            _settings = settings;
            _mappingPhase = mappingPhase;
            _reducingPhase = reducingPhase;
            _partitioningPhase = partitioningPhase;
            _workerInfoDto = new()
            {
                WorkerUuid = _workerUuid
            };
        }

        public async Task StartAsync(CancellationToken cancelToken)
        {
            using var grpcChannel = MRRpcClientFactory.CreateGrpcChannel();
            var rpcClient = MRRpcClientFactory.CreaterpcClient(grpcChannel);

            using var heartBeatTicker = HeartBeatLoop(rpcClient, cancelToken);
            var workTask = Task.Run(() => WorkLoopAsync(rpcClient, cancelToken), cancelToken);

            await Task.WhenAll(workTask).ConfigureAwait(false);
        }

        private System.Timers.Timer HeartBeatLoop(
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient, CancellationToken cancelToken)
        {
            System.Timers.Timer heartBeatTicker = new()
            {
                Interval = TimeSpan.FromSeconds(4).TotalMilliseconds
            };
            heartBeatTicker.Elapsed += (object sender, ElapsedEventArgs e) =>
            {
                if (!cancelToken.IsCancellationRequested)
                {
                    try
                    {
                        _ = rpcClient.HeartBeatAsync(_workerInfoDto, cancellationToken: cancelToken);
                    }
                    catch (RpcException) { }
                }
            };
            heartBeatTicker.Start();
            return heartBeatTicker;
        }

        private async Task WorkLoopAsync(
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient, CancellationToken cancelToken)
        {
            while (!cancelToken.IsCancellationRequested)
            {
                // fetch task from master
                try
                {
                    var taskInfoDto = await rpcClient.AskForTaskAsync(_workerInfoDto, cancellationToken: cancelToken);

                    switch ((MapReduceTaskType)taskInfoDto.TaskType)
                    {
                        case MapReduceTaskType.Map:
                            Mapper<TKey, TValue> mapper = new(
                                mappingPhase: _mappingPhase,
                                partitioningPhase: _partitioningPhase,
                                rpcClient: rpcClient,
                                settings: _settings);
                            await mapper.StartAsync(
                                inputFilePath: taskInfoDto.InputFileInfo.FilePath,
                                taskId: taskInfoDto.TaskId,
                                numPartitions: taskInfoDto.ReduceTaskCount
                            ).ConfigureAwait(false);
                            break;
                        case MapReduceTaskType.Reduce:
                            Reducer<TKey, TValue> reducer = new(
                                reducingPhase: _reducingPhase,
                                rpcClient: rpcClient,
                                settings: _settings);
                            await reducer.StartAsync(
                                intermediateFilePaths: taskInfoDto.IntermediateFilesInfos.Select(xxxx => xxxx.FilePath).ToList(),
                                taskId: taskInfoDto.TaskId,
                                partitionIndex: taskInfoDto.PartitionIndex
                            ).ConfigureAwait(false);
                            break;
                        case MapReduceTaskType.Exit:
                        default:
                            await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                            break;
                    }
                }
                catch (RpcException) {
                    await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                }
            }
        }
    }
}
