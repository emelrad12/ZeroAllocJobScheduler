using System.Diagnostics;

namespace Schedulers.Utils;

public interface IHasName
{
    /// <summary>
    /// Returns a user-friendly name of the job.
    /// </summary>
    string GetName()
    {
        return GetType().Name;
    }
}

public struct WorkerPerformanceEvent
{
    public int workerId;
    public long startTimeStamp;
    public long endTimeStamp;
    public IHasName? jobType;
}

public class WorkersPerformanceEvents
{
    public Dictionary<int, List<WorkerPerformanceEvent>> Data = new();

    public WorkersPerformanceEvents()
    {
        var scheduler = SchedulerCommon.GlobalScheduler;
        foreach (var worker in scheduler.Workers)
        {
            var queue = worker.PerformanceEvents;
            Data[worker.WorkerId + 2] = queue.ToList();
        }

        Data[0] = MainThreadPerformanceEvents.events.ToList();
        Data[1] = MainThreadPerformanceEvents.events2.ToList();
    }
}

public static class MainThreadPerformanceEvents
{
    internal static List<WorkerPerformanceEvent> events = [];
    internal static List<WorkerPerformanceEvent> events2 = [];

    public static void Clear()
    {
        var scheduler = SchedulerCommon.GlobalScheduler;
        foreach (var worker in scheduler.Workers)
        {
            worker.PerformanceEvents.Clear();
        }

        events.Clear();
        events2.Clear();
    }

    public static WorkerPerformanceEvent StartRecording(IHasName type, int workerId)
    {
        return new() { jobType = type, startTimeStamp = Stopwatch.GetTimestamp(), workerId = workerId };
    }

    public static void Submit(WorkerPerformanceEvent data)
    {
        data.endTimeStamp = Stopwatch.GetTimestamp();
        if (data.workerId == 0)
        {
            events.Add(data);
        }
        else if (data.workerId == 1)
        {
            events2.Add(data);
        }
        else
        {
            throw new InvalidOperationException();
        }
    }
}
