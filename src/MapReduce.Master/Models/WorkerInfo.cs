namespace MapReduce.Master.Models
{
    public class WorkerInfo
    {
        public string WorkerUuid { get; set; }
        public WorkerStatus State { get; set; }
    }
}
