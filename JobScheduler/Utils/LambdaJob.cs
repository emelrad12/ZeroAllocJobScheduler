namespace Schedulers.Utils;

/// <summary>
/// Simple class to schedule lambdas as jobs.
/// Similar to Task.Run, but uses the job system.
/// </summary>
public class LambdaJob
{
    private readonly struct JobProducer(Action action) : IJob
    {
        public void Execute()
        {
            action();
        }

        public string GetName()
        {
            return (action.Method.DeclaringType?.FullName ?? "Unknown") + "." + action.Method.Name;
        }
    }

    /// <summary>
    /// Creates a new job from a lambda expression.
    /// </summary>
    /// <param name="action">The function to run</param>
    /// <param name="source">The source handle, that will trigger this job</param>
    /// <returns></returns>
    public static JobHandle Create(Action action, JobHandle source = default)
    {
        var job = new JobProducer(action);
        var handle = SchedulerCommon.GlobalScheduler!.Schedule(job);
        if (!source.IsNull)
        {
            handle.SetDependsOn(source);
        }

        return handle;
    }

    /// <summary>
    /// Wrapper around <see cref="Create"/> that flushes the job and waits for it to complete.
    /// </summary>
    public static void CreateFlushAndWait(Action action)
    {
        var handle = Create(action);
        handle.FlushAndWait();
    }
}
