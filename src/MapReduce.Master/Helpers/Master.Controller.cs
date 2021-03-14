using System;
using System.Threading.Tasks;
using MapReduce.Shared;

namespace MapReduce.Master.Helpers
{
    public partial class Master
    {
        public Task<TaskInfoDto> AskForTaskAsync(WorkerInfoDto request)
        {
            return Task.FromResult(this.AssignTask(request));
        }

        public Task<Empty> HeartBeatAsync(WorkerInfoDto request)
        {
            lock (_workers)
            {
                var worker = _workers.Find(xxxx => xxxx.WorkerUuid == request.WorkerUuid);
                if (worker == null)
                {
                    _workers.Add(new()
                    {
                        LastHeartBeatTime = DateTime.UtcNow,
                        WorkerUuid = request.WorkerUuid
                    });
                }
                else
                {
                    worker.LastHeartBeatTime = DateTime.UtcNow;
                }
            }
            return Task.FromResult(new Empty());
        }

        public Task<Empty> MapDoneAsync(MapOutputInfoDto request)
        {
            var mapTask = _mapTasks.Find(xxxx => xxxx.Assignee.WorkerUuid == request.WorkerInfo.WorkerUuid);
            if (mapTask != null)
            {
                mapTask.Assignee.AssignedTask = null;
                mapTask.Assignee = null;
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

        public Task<Empty> ReduceDoneAsync(ReduceOutputInfoDto request)
        {
            var reduceTask = _reduceTasks.Find(xxxx => xxxx.Assignee.WorkerUuid == request.WorkerInfo.WorkerUuid);
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
            return Task.FromResult(new Empty());
        }
    }
}
