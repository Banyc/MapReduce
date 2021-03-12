namespace MapReduce.Master.Models
{
    public abstract class MapReduceTask
    {
        // assigned by master
        public int TaskId { get; set; }
        // null when no assignee
        public WorkerInfo Assignee { get; set; }

        public abstract bool IsTaskCompleted { get; }
    }
}
