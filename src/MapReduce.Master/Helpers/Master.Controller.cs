using System.Collections.Generic;
using System;
using System.Threading.Tasks;
using MapReduce.Shared;
using MapReduce.Master.Models;

namespace MapReduce.Master.Helpers
{
    public partial class Master
    {
        public Task<TaskInfoDto> AskForTaskAsync(WorkerInfoDto request)
        {
            Console.WriteLine($"[info] {request.WorkerUuid}: Ask for task.");
            return Task.FromResult(this.AssignTask(request));
        }

        public Task<Empty> HeartbeatAsync(WorkerInfoDto request)
        {
            Console.WriteLine($"[info] {request.WorkerUuid}: Heartbeat.");
            lock (_workers)
            {
                var worker = _workers.Find(xxxx => xxxx.WorkerUuid == request.WorkerUuid);
                if (worker == null)
                {
                    _workers.Add(new()
                    {
                        LastHeartbeatTime = DateTime.UtcNow,
                        WorkerUuid = request.WorkerUuid
                    });
                }
                else
                {
                    worker.LastHeartbeatTime = DateTime.UtcNow;
                }
            }
            return Task.FromResult(new Empty());
        }

        public Task<Empty> MapDoneAsync(MapOutputInfoDto request)
        {
            Console.WriteLine($"[info] {request.WorkerInfo.WorkerUuid}: Map done.");
            Console.WriteLine($"[info] {request.WorkerInfo.WorkerUuid}: Files are:");
            foreach (var fileInfo in request.FileInfos)
            {
                Console.WriteLine($"[info] {fileInfo.FilePath}");
            }
            lock (_mapTasks)
            {
                var mapTask = _mapTasks.Find(xxxx => xxxx.Assignee?.WorkerUuid == request.WorkerInfo.WorkerUuid);
                if (mapTask != null)
                {
                    mapTask.Assignee.AssignedTask = null;
                    mapTask.Assignee = null;
                    mapTask.CompletedFileInfos = new List<SimpleFileInfo>();
                    foreach (var fileInfo in request.FileInfos)
                    {
                        mapTask.CompletedFileInfos.Add(new()
                        {
                            FilePath = fileInfo.FilePath,
                            FileSize = fileInfo.FileSize,
                            PartitionIndex = fileInfo.PartitionIndex
                        });
                    }
                }
                return Task.FromResult(new Empty());
            }
        }

        public Task<Empty> ReduceDoneAsync(ReduceOutputInfoDto request)
        {
            Console.WriteLine($"[info] {request.WorkerInfo.WorkerUuid}: Reduce done.");
            Console.WriteLine($"[info] {request.WorkerInfo.WorkerUuid}: File is:");
            Console.WriteLine($"[info] {request.FileInfo.FilePath}");
            lock (_reduceTasks)
            {
                var reduceTask = _reduceTasks.Find(xxxx => xxxx.Assignee?.WorkerUuid == request.WorkerInfo.WorkerUuid);
                if (reduceTask != null)
                {
                    reduceTask.Assignee.AssignedTask = null;
                    reduceTask.Assignee = null;
                    reduceTask.CompletedFileInfo = new()
                    {
                        FilePath = request.FileInfo.FilePath,
                        FileSize = request.FileInfo.FileSize,
                        PartitionIndex = request.FileInfo.PartitionIndex
                    };
                }
                // check if all tasks are done
                if (!_reduceTasks.Exists(xxxx => !xxxx.IsTaskCompleted))
                {
                    _isAllDone = true;
                }
                return Task.FromResult(new Empty());
            }
        }
    }
}
