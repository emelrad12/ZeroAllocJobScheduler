[assembly: CLSCompliant(true)]

namespace Schedulers;

/// <summary>
///     A <see cref="JobScheduler"/> schedules and processes <see cref="IJob"/>s asynchronously. Better-suited for larger jobs due to its underlying events.
/// </summary>
public class JobScheduler : IDisposable
{
    /// <summary>
    /// Creates an instance and singleton.
    /// </summary>
    /// <param name="threads">The amount of worker threads to use.
    /// If zero we will use the amount of processors available.
    /// Any negative value means no threads. Useful for debugging/testing.</param>
    public JobScheduler(int threads = 0)
    {
        var amount = threads;
        if (amount == 0)
        {
            amount = Environment.ProcessorCount;
        }

        for (var index = 0; index < amount; index++)
        {
            var worker = new Worker(this, index);
            Workers.Add(worker);
            Queues.Add(worker.Queue);
        }

        foreach (var worker in Workers)
        {
            worker.Start();
        }
    }

    /// <summary>
    /// A list of all <see cref="WorkStealingDeque{T}"/> of the Workers with <see cref="IJob"/>s that are currently being processed.
    /// </summary>
    internal List<WorkStealingDeque<JobHandle>> Queues { get; } = new();

    /// <summary>
    /// All active <see cref="Workers"/>.
    /// </summary>
    internal List<Worker> Workers { get; } = new();

    /// <summary>
    /// An index pointing towards the next worker being used to process the next flushed <see cref="IJob"/>.
    /// </summary>
    internal int NextWorkerIndex { get; set; }

    public static int totalRequested;
    /// <summary>
    /// Creates a new <see cref="JobHandle"/> from a <see cref="IJob"/>.
    /// </summary>
    /// <param name="iJob">The <see cref="IJob"/>.</param>
    /// <returns>The new created <see cref="JobHandle"/>.</returns>
    public JobHandle Schedule(IJob? iJob = null)
    {
        totalRequested++;
        return JobHandle.Pool.RentJobHandle(iJob);
    }

    /// <summary>
    /// Creates a new <see cref="JobHandle"/> from a <see cref="IJob"/> with a <see cref="JobHandle"/> as a parent.
    /// Links the job to the parent. This allows you to wait for the parent until all of its children have been processed.
    /// </summary>
    /// <param name="iJob">The <see cref="IJob"/>.</param>
    /// <param name="parent">The parent <see cref="JobHandle"/>.</param>
    /// <returns>The new <see cref="JobHandle"/>.</returns>
    public JobHandle Schedule(IJob? iJob, JobHandle parent)
    {
        totalRequested++;
        var job = JobHandle.Pool.RentJobHandle(iJob);
        job.SetParent(parent);
        return job;
    }

    /// <summary>
    /// Schedule a new <see cref="JobHandle"/> with a parent <see cref="JobHandle"/>.
    /// Simple a helper function to avoid writing (null, parent) every time.
    /// </summary>
    public JobHandle Schedule(JobHandle parent)
    {
        totalRequested++;
        var job = JobHandle.Pool.RentJobHandle();
        job.SetParent(parent);
        return job;
    }

    /// <summary>
    /// Creates a dependency between two <see cref="JobHandle"/>s.
    /// The dependent <see cref="JobHandle"/>s is executed after the target <see cref="JobHandle"/> has finished.
    /// This ensures that a <see cref="JobHandle"/> is executed at a later point in time.
    /// </summary>
    /// <param name="dependency">The <see cref="JobHandle"/> dependency.</param>
    /// <param name="dependOn">The <see cref="JobHandle"/> it depends on.</param>
    public void AddDependency(JobHandle dependency, JobHandle dependOn)
    {
        dependency.SetDependsOn(dependOn);
    }

    /// <summary>
    /// A function that calculates the current thread usage of the <see cref="Workers"/>.
    /// It is very simple and just counts the amount of <see cref="Worker"/>s that are currently working.
    /// </summary>
    /// <returns>a value between 0 - 1</returns>
    public float CalculateThreadUsage()
    {
        var total = 0f;
        foreach (var worker in Workers)
        {
            total += worker.IsCurrentlyWorking ? 1 : 0;
        }

        total /= Workers.Count;
        return total;
    }

    /// <summary>
    /// Tries to transfer <see cref="JobHandle"/> to the <see cref="Workers"/> so that it can be executed.
    /// If it has unfinished children it will not be flushed, and instead will be left for the children to flush.
    /// </summary>
    /// <param name="job">The <see cref="JobHandle"/>.</param>
    public void Flush(JobHandle job)
    {
        // This is to prevent the job from being flushed multiple times.
        var unfinishedJobs = job.SetReadyToExecute();

        if (unfinishedJobs != 1)
        {
            return;
        }

        // It is pointless to flush a job that has no work to do.
        // This is a job that triggers other jobs,
        // so might as well do it now, to lower latency.
        if (job.Job == null)
        {
            Finish(job);
            return;
        }

        while (!Worker.Enqueue(job))
        {
        }
    }

    /// <summary>
    /// Wait until a <see cref="JobHandle"/> and all its children have been completed.
    /// Also works on jobs in the meantime.
    /// </summary>
    /// <param name="job">The <see cref="JobHandle"/>.</param>
    public void Wait(JobHandle job)
    {
        while (!job.IsFinished())
        {
            var nextJob = Worker.TryStealJobExternal(out var stolenJob);
            if (!nextJob)
            {
                for (var i = 0; i < Workers.Count; i++)
                {
                    nextJob = Workers[i].Queue.TrySteal(out stolenJob);
                    if (nextJob)
                    {
                        break;
                    }
                }
            }

            if (nextJob)
            {
                stolenJob.Job?.Execute();
                Finish(stolenJob);
            }

            // Dont yield if you can find something to execute.
            if (!nextJob)
            {
                Thread.Yield();
            }
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="handle">The job handle to finish</param>
    private void OnChildFinish(JobHandle handle)
    {
        var unfinishedJobs = Interlocked.Decrement(ref handle.UnfinishedJobs);
        if (unfinishedJobs < 1)
        {
            throw new InvalidOperationException($"Unfinished jobs cannot be negative, id:{handle.Index} job: {handle.Job == null}, unfinished jobs: {unfinishedJobs}");
        }

        if (unfinishedJobs == 1)
        {
            Worker.Enqueue(handle);
            // handle.Job?.Execute();
            // Finish(handle);
        }
    }

    /// <summary>
    /// Finalizes a <see cref="JobHandle"/> and processes its dependencies.
    /// </summary>
    /// <param name="handle">The job handle to finish</param>
    internal void Finish(JobHandle handle)
    {
        if (handle.UnfinishedJobs != 1)
        {
            throw new($"Finish called on a job with {handle.UnfinishedJobs} != 1 unfinished jobs, id:{handle.Index} job: {handle.Job == null}");
        }

        if (handle.Parent != JobHandle.NullHandleId)
        {
            OnChildFinish(new(handle.Parent));
        }

        if (handle.HasDependents())
        {
            var dependents = handle.GetDependents();
            foreach (var id in dependents)
            {
                if (id == JobHandle.NullHandleId)
                {
                    continue;
                }

                OnChildFinish(new(id));
            }
        }

        Interlocked.Decrement(ref handle.UnfinishedJobs);

        JobHandle.Pool.ReturnHandle(handle);
    }

    /// <summary>
    /// Cleans this instance and terminates all <see cref="Worker"/>s.
    /// </summary>
    public void Dispose()
    {
        foreach (var worker in Workers)
        {
            worker.Stop();
        }
    }
}
