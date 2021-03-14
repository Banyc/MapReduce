using System;
using System.Threading;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using MapReduce.Master.Models;
using MapReduce.Shared;
using MapReduce.Shared.Helpers;
using System.Threading.Tasks;
using MapReduce.Master.Controllers;
using Grpc.Core;

namespace MapReduce.Master.Helpers
{
    public partial class Master
    {
        // worker list
        private readonly List<WorkerInfo> _workers = new();
        // map tasks
        private readonly List<MapTask> _mapTasks = new();
        // reduce tasks
        private readonly List<ReduceTask> _reduceTasks = new();
        private int _biggestTaskId = 0;
        private readonly MasterSettings _settings;
        private Grpc.Core.Server _rpcServer;
        private bool _isAllDone = false;

        public Master(MasterSettings settings)
        {
            _settings = settings;
            lock (_mapTasks)
            {
                // build _mapTasks
                foreach (var inputFilePath in settings.InputFilePaths)
                {
                    _mapTasks.Add(new()
                    {
                        // AssignedFilePath = inputFilePath
                        AssignedFile = new FileInfo(inputFilePath)
                    });
                }
            }
            lock (_reduceTasks)
            {
                // build _reduceTasks
                int i;
                for (i = 0; i < settings.ReduceTaskCount; i++)
                {
                    _reduceTasks.Add(new()
                    {

                    });
                }
            }
            // build listener
            MapReduceController controller = new(this);
            _rpcServer = new()
            {
                Services = { RpcMapReduceService.BindService(controller) },
                Ports = { new ServerPort(_settings.IpAddress, _settings.Port, ServerCredentials.Insecure) }
            };
        }

        public async Task StartAsync(CancellationToken cancelToken)
        {
            // activate controller
            _rpcServer.Start();

            // check heart beats from clients
            Task healthManagerTask = Task.Run(() => StartWorkerHealthManager(cancelToken), cancelToken);

            await Task.WhenAll(healthManagerTask).ConfigureAwait(false);
            await _rpcServer.ShutdownAsync().ConfigureAwait(false);
        }

        public async Task StartWorkerHealthManager(CancellationToken cancelToken)
        {
            while (!cancelToken.IsCancellationRequested && !_isAllDone)
            {
                lock (_workers)
                {
                    lock (_mapTasks)
                    {
                        lock (_reduceTasks)
                        {
                            int i;
                            for (i = _workers.Count - 1; i >= 0; i--)
                            {
                                var worker = _workers[i];
                                if (DateTime.UtcNow - worker.LastHeartBeatTime > TimeSpan.FromSeconds(10))
                                {
                                    if (worker.AssignedTask != null)
                                    {
                                        worker.AssignedTask.Assignee = null;
                                        worker.AssignedTask = null;
                                    }
                                    _workers.RemoveAt(i);
                                }
                            }
                        }
                    }
                }
                await Task.Delay(TimeSpan.FromSeconds(10), cancelToken).ConfigureAwait(false);
            }
        }

        public TaskInfoDto AssignTask(WorkerInfoDto workerInfoDto)
        {
            WorkerInfo workerInfo = _workers.Find(xxxx => xxxx.WorkerUuid == workerInfoDto.WorkerUuid);
            if (workerInfo == null)
            {
                // the worker is not registered. Ignore.
                return new()
                {
                    TaskType = (int)MapReduceTaskType.Nop
                };
            }
            lock (this)
            {
                _biggestTaskId++;
            }
            // assign DTO with info
            TaskInfoDto taskInfoDto = new();
            taskInfoDto.TaskId = _biggestTaskId;
            taskInfoDto.ReduceTaskCount = _reduceTasks.Count;
            // if map not done
            if (!_mapTasks.All(xxxx => xxxx.IsTaskCompleted))
            {
                lock (_mapTasks)
                {
                    // assign workers with map tasks
                    taskInfoDto.TaskType = (int)MapReduceTaskType.Map;
                    // find unassigned task
                    MapTask mapTask = _mapTasks.Find(xxxx => !xxxx.IsTaskCompleted && xxxx.Assignee == null);
                    if (mapTask == null)
                    {
                        // there is task not completed and those uncompleted tasks have already been assigned with some workers
                        return new()
                        {
                            TaskType = (int)MapReduceTaskType.Nop,
                            TaskId = _biggestTaskId
                        };
                    }
                    mapTask.Assignee = workerInfo;
                    mapTask.TaskId = _biggestTaskId;
                    // assign the task to the worker
                    workerInfo.AssignedTask = mapTask;
                    // build DTO file info
                    FileInfoDto fileInfoDto = new()
                    {
                        FilePath = mapTask.AssignedFile.FullName,
                        FileSize = (int)mapTask.AssignedFile.Length
                    };
                    taskInfoDto.InputFileInfo = fileInfoDto;
                }
            }
            else if (!_reduceTasks.All(xxxx => xxxx.IsTaskCompleted))
            {
                lock (_reduceTasks)
                {
                    // assign workers with reduce tasks
                    taskInfoDto.TaskType = (int)MapReduceTaskType.Reduce;
                    // find unassigned task
                    ReduceTask reduceTask = _reduceTasks.Find(xxxx => !xxxx.IsTaskCompleted && xxxx.Assignee == null);
                    if (reduceTask == null)
                    {
                        // there is task not completed and those uncompleted tasks have already been assigned with some workers
                        return new()
                        {
                            TaskType = (int)MapReduceTaskType.Nop,
                            TaskId = _biggestTaskId
                        };
                    }
                    int partitionIndex = _reduceTasks.IndexOf(reduceTask);
                    reduceTask.Assignee = workerInfo;
                    reduceTask.TaskId = _biggestTaskId;
                    // assign the task to the worker
                    workerInfo.AssignedTask = reduceTask;
                    // assign partition
                    reduceTask.AssignedFiles = new List<SimpleFileInfo>();
                    foreach (var mapTask in _mapTasks)
                    {
                        var file = mapTask.CompletedFileInfos
                            .First(xxxx => xxxx.PartitionIndex == partitionIndex);
                        reduceTask.AssignedFiles.Add(file);
                    }
                    // build DTO file info
                    foreach (var assignedFile in reduceTask.AssignedFiles)
                    {
                        FileInfoDto fileInfoDto = new()
                        {
                            FilePath = assignedFile.FilePath,
                            FileSize = assignedFile.FileSize,
                            PartitionIndex = assignedFile.PartitionIndex
                        };
                        taskInfoDto.IntermediateFilesInfos.Add(fileInfoDto);
                    }
                    // assign DTO with partition index
                    taskInfoDto.PartitionIndex = partitionIndex;
                }
            }
            else
            {
                return new()
                {
                    TaskType = (int)MapReduceTaskType.Nop,
                    TaskId = _biggestTaskId
                };
            }

            return taskInfoDto;
        }
    }
}
