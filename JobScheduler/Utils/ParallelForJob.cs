namespace Schedulers.Utils;

/// <summary>
/// A utility class for creating parallel jobs that execute an action for each index in a range.
/// </summary>
public static class ParallelForJob
{
    private readonly struct JobProducer(Action<int> action) : IParallelJobProducer
    {
        public void RunSingle(int index)
        {
            action(index);
        }
    }

    /// <summary>
    /// Creates a ParallelJobProducer that executes the provided action in parallel for the specified count.
    /// </summary>
    /// <param name="start">Where to start executing</param>
    /// <param name="count">Executes to this</param>
    /// <param name="action">What to run</param>
    /// <param name="target">The job to trigger after finishing</param>
    /// <param name="source">The job that will trigger this one</param>
    /// <returns>A struct that wraps the handle</returns>
    public static JobHandle Create(int start, int count, Action<int> action, JobHandle target = default, JobHandle source = default)
    {
        var producer = new ParallelJobProducer<JobProducer>(start, count, new(action), source: source);
        if (!target.IsNull)
        {
            producer.GetHandle().SetParent(target);
        }
        return producer.GetHandle();
    }

    /// <summary>
    /// Wrapper around <see cref="Create"/> that flushes the job and waits for it to complete.
    /// </summary>
    public static void CreateFlushAndWait(int start, int count, Action<int> action)
    {
        var handle = Create(start, count, action, JobHandle.Null);
        handle.FlushAndWait();
    }
}
