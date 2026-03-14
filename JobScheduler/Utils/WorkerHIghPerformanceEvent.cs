namespace Schedulers.Utils;

public struct WorkerPerformanceEvent
{
    public int workerId;
    public long startTimeStamp;
    public long endTimeStamp;
    public IJob? jobType;
}

public class WorkerHighPerformanceEventData
{
    public Dictionary<int, List<WorkerPerformanceEvent>> Data = new();

    public WorkerHighPerformanceEventData()
    {
        var scheduler = ParallelForJobCommon.GlobalScheduler;
        foreach (var worker in scheduler.Workers)
        {
            var queue = worker.PerformanceEvents;
            Data[worker.WorkerId] = queue.ToList();
        }
    }
}
