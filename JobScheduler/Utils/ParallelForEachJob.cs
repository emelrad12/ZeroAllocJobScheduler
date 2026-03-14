using System.Collections.Concurrent;

namespace Schedulers.Utils;

/// <summary>
/// A utility class for running actions in parallel over a collection of items.
/// </summary>
public static class ParallelForEachJob
{
    private readonly struct PartitionedJobProducer<T>(IList<IEnumerator<T>> partitions, Action<T> action) : IParallelJobProducer
    {
        public void RunSingle(int index)
        {
            var enumerator = partitions[index];
            while (enumerator.MoveNext())
            {
                action(enumerator.Current);
            }
        }

        public string GetName()
        {
            return action.Method.DeclaringType?.FullName ?? "Unknown";
        }
    }

    /// <summary>
    /// Creates a ParallelJobProducer that executes the provided action on every item of the given collection.
    /// Don't call with 0 long collections. Filter out before calling.
    /// </summary>
    /// <param name="data">The data to run the scheduler over</param>
    /// <param name="action">What to run</param>
    /// <param name="source">The job that will trigger this one</param>
    /// <param name="parent"></param>
    /// <returns>A struct that wraps the handle</returns>
    public static JobHandle Create<T>(IEnumerable<T> data, Action<T> action, JobHandle parent = default, JobHandle source = default)
    {
        var partitioner = Partitioner.Create(data);
        // We use Environment.ProcessorCount * 4 to account for uneven distribution of work and to ensure that we have enough partitions to keep all cores busy.
        // The overhead of jobs is very low hence we can afford to have more partitions than cores.
        var partitions = partitioner.GetPartitions(Environment.ProcessorCount * 2);
        var producer = new ParallelJobProducer<PartitionedJobProducer<T>>(0, partitions.Count, new(partitions, action), source: source);
        if (!parent.IsNull)
        {
            producer.GetHandle().SetParent(parent);
        }

        return producer.GetHandle();
    }

    /// <summary>
    /// Wrapper around <see cref="ParallelForEachJob.Create"/> that flushes the job and waits for it to complete.
    /// Don't call with 0 long collections. Filter out before calling.
    /// </summary>
    public static void CreateFlushAndWait<T>(IEnumerable<T> source, Action<T> action)
    {
        var handle = Create(source, action, JobHandle.Null);
        handle.FlushAndWait();
    }
}
