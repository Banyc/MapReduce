namespace MapReduce.Worker.Models
{
    public class WorkerSettings
    {
        public string WorkerUuid { get; set; }
        public string ReducedOutputDirectory { get; set; }
        public string MappedOutputDirectory { get; set; }
    }
}
