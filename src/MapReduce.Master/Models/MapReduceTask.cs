namespace MapReduce.Master.Models
{
    public abstract class MapReduceTask
    {
        // assigned by master
        public int TaskId { get; set; }
        // null when no assignee
        public WorkerInfo Assignee { get; set; }

        public MapReduceTaskStatus State
        {
            get
            {
                if (this.IsTaskCompleted)
                {
                    return MapReduceTaskStatus.Completed;
                }
                else if (this.Assignee != null)
                {
                    return MapReduceTaskStatus.InProgress;
                }
                else
                {
                    return MapReduceTaskStatus.Idle;
                }
            }
        }
        public abstract bool IsTaskCompleted { get; }
    }
}
