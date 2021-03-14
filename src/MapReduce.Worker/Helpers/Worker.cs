using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Grpc.Core;
using Grpc.Net.Client;
using MapReduce.Shared;
using MapReduce.Shared.Helpers;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Worker<TKey, TValue> : IDisposable
    {
        private readonly string _workerUuid = Guid.NewGuid().ToString();
        private readonly WorkerInfoDto _workerInfoDto;
        private readonly WorkerSettings _settings;

        private readonly IMapping<TKey, TValue> _mappingPhase;
        private readonly IReducing<TKey, TValue> _reducingPhase;
        private readonly IPartitioning<TKey, TValue> _partitioningPhase;
        private readonly GrpcChannel _channel;
        private readonly System.Timers.Timer _heartBeatTicker;
        private readonly RpcClientFactory _rpcClientFactory;
        public bool IsWorking { get; private set; }

        public Worker(
            WorkerSettings settings,
            RpcClientFactory rpcClientFactory,
            IMapping<TKey, TValue> mappingPhase,
            IReducing<TKey, TValue> reducingPhase,
            IPartitioning<TKey, TValue> partitioningPhase)
        {
            _settings = settings;
            _rpcClientFactory = rpcClientFactory;
            _mappingPhase = mappingPhase;
            _reducingPhase = reducingPhase;
            _partitioningPhase = partitioningPhase;
            _workerInfoDto = new()
            {
                WorkerUuid = _workerUuid
            };
            _channel = rpcClientFactory.CreateRpcChannel();
            _heartBeatTicker = new()
            {
                Interval = TimeSpan.FromSeconds(4).TotalMilliseconds
            };
            StartHeartBeat();
        }

        public void Dispose()
        {
            _heartBeatTicker.Dispose();
            _channel.Dispose();
        }

        public void StartHeartBeat()
        {
            // heart beats
            var rpcClientHeartBeat = RpcClientFactory.CreateRpcClient(_channel);

            _heartBeatTicker.Elapsed += (object sender, ElapsedEventArgs e) => _ = SendHeartBeatAsync(rpcClientHeartBeat);
            _heartBeatTicker.Start();
        }

        public async Task StartAsync(CancellationToken cancelToken)
        {
            // worker tasks
            if (this.IsWorking)
            {
                return;
            }
            this.IsWorking = true;

            var rpcClientWorkTask = RpcClientFactory.CreateRpcClient(_channel);
            var workTask = Task.Run(() => WorkLoopAsync(rpcClientWorkTask, cancelToken), cancelToken);

            try
            {
                await Task.WhenAll(workTask).ConfigureAwait(false);
            }
            finally
            {
                this.IsWorking = false;
            }
        }

        private async Task SendHeartBeatAsync(
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient)
        {
            try
            {
                _ = await rpcClient.HeartBeatAsync(_workerInfoDto);
            }
            catch (RpcException) { }
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
                catch (RpcException)
                {
                    await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                }
            }
        }
    }
}
