using System;
using System.Threading.Tasks;
using Grpc.Core;
using MapReduce.Shared;

namespace MapReduce.Master.Controllers
{
    public class MapReduceController : RpcMapReduceService.RpcMapReduceServiceBase
    {
        private readonly Helpers.Master _master;
        public MapReduceController(Helpers.Master master)
        {
            _master = master;
        }

        public override Task<TaskInfoDto> AskForTask(WorkerInfoDto request, ServerCallContext context)
        {
            return _master.AskForTaskAsync(request);
        }

        public override Task<Empty> Heartbeat(WorkerInfoDto request, ServerCallContext context)
        {
            return _master.HeartbeatAsync(request);
        }

        public override Task<Empty> MapDone(MapOutputInfoDto request, ServerCallContext context)
        {
            return _master.MapDoneAsync(request);
        }

        public override Task<Empty> ReduceDone(ReduceOutputInfoDto request, ServerCallContext context)
        {
            return _master.ReduceDoneAsync(request);
        }
    }
}
