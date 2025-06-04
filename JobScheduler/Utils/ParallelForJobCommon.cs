namespace Schedulers.Utils;

/// <summary>
/// Common utilities for parallel jobs.
/// </summary>
public static class ParallelForJobCommon
{
    internal static JobScheduler? GlobalScheduler;

    /// <summary>
    /// Sets the default scheduler for parallel jobs.
    /// </summary>
    public static void SetScheduler(JobScheduler? scheduler)
    {
        if (GlobalScheduler != null)
        {
            throw new InvalidOperationException("Global scheduler is already set.");
        }

        GlobalScheduler = scheduler;
    }

    /// <summary>
    /// Returns the current global scheduler.
    /// </summary>
    public static JobScheduler GetCurrent()
    {
        return GlobalScheduler ?? throw new InvalidOperationException("Global scheduler is not initialized. Call SetScheduler first.");
    }

    /// <summary>
    /// Disposes the global scheduler and sets it to null.
    /// </summary>
    public static void DisposeScheduler()
    {
        if (GlobalScheduler == null)
        {
            throw new InvalidOperationException("Global scheduler is not initialized.");
        }

        GlobalScheduler.Dispose();
        GlobalScheduler = null;
    }
}
