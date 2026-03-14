using System.Diagnostics;
using Schedulers.Utils;

namespace Schedulers;

/// <summary>
/// Represents a thread which has a <see cref="WorkStealingDeque{T}"/> and processes <see cref="JobHandle"/>s.
/// Steals <see cref="JobHandle"/>s from other workers if it has nothing more to do.
/// </summary>
internal class Worker
{
    public readonly int WorkerId;
    private readonly Thread _thread;

    /// <summary>
    /// Use a single combined <see cref="UnorderedThreadSafeQueue{T}"/> for all <see cref="Worker"/>s to enqueue <see cref="JobHandle"/>s.
    /// Because there really isn't performance advantage to having multiple queues for each <see cref="Worker"/>,
    /// </summary>
    private static readonly UnorderedThreadSafeQueue<JobHandle> _incomingQueue = new();

    private readonly WorkStealingDeque<JobHandle> _queue;
    private readonly JobScheduler _jobScheduler;
    private volatile CancellationTokenSource _cancellationToken;
    public bool IsCurrentlyWorking { get; private set; } = false;

    // use a high spin count to avoid sleeping the thread under variable load.
    // 2047 is the maximum value for a ManualResetEventSlim spin count.
    private readonly static ManualResetEventSlim _workAvailable = new(false, 2047);
    public readonly Queue<WorkerPerformanceEvent> PerformanceEvents = new(10_000_000);

    /// <summary>
    /// Creates a new <see cref="Worker"/>.
    /// </summary>
    /// <param name="jobScheduler">Its <see cref="JobScheduler"/>.</param>
    /// <param name="id">Its <see cref="WorkerId"/>.</param>
    public Worker(JobScheduler jobScheduler, int id)
    {
        WorkerId = id;

        _queue = new(32);
        _jobScheduler = jobScheduler;
        _cancellationToken = new();

        _thread = new(() => Run(_cancellationToken.Token));
        _thread.Name = $"Arch Worker #{WorkerId}";
    }

    public static bool Enqueue(JobHandle handle)
    {
        var result = _incomingQueue.TryEnqueue(handle);
        _workAvailable.Set();
        return result;
    }

    /// <summary>
    /// Its <see cref="WorkStealingDeque{T}"/> with <see cref="JobHandle"/>s to process.
    /// </summary>
    public WorkStealingDeque<JobHandle> Queue
    {
        get => _queue;
    }

    /// <summary>
    /// Starts this instance.
    /// </summary>
    public void Start()
    {
        _thread.Start();
    }

    /// <summary>
    /// Stops this instance.
    /// </summary>
    public void Stop()
    {
        _cancellationToken.Cancel();
        _workAvailable.Set();
    }

    /// <summary>
    /// Try to get a job from the <see cref="_incomingQueue"/> and remove it from the queue.
    /// As this method is most likely used on a wait, it also tries to flush the <see cref="_bulkQueue"/> first,
    /// to prevent deadlocks, when the thread #0 is waiting for jobs to be enqueued, but it is also the one supposed to be quickly enqueuing jobs into the queue.
    /// </summary>
    public static bool TryStealJobExternal(out JobHandle job)
    {
        return _incomingQueue.TryDequeue(out job);
    }

    /// <summary>
    /// Runs this instance to process its <see cref="JobHandle"/>s.
    /// Steals from other <see cref="Worker"/>s if its own <see cref="_queue"/> is empty.
    /// </summary>
    /// <param name="token"></param>
    private void Run(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var profilingEnabled = ParallelForJobCommon.ProfilingEnabled;
                var noWorkFound = true;
                // Pass jobs to the local queue
                while (_queue.Size() < 32 && _incomingQueue.TryDequeue(out var jobHandle))
                {
                    _queue.PushBottom(jobHandle);
                    noWorkFound = false;
                }

                var currentPerformanceEvent = new WorkerPerformanceEvent();
                // Process job in own queue
                var exists = _queue.TryPopBottom(out var job);
                if (exists)
                {
                    if (profilingEnabled)
                    {
                        currentPerformanceEvent = new() { workerId = WorkerId, jobType = job.Job, startTimeStamp = Stopwatch.GetTimestamp() };
                        IsCurrentlyWorking = true;
                    }

                    job.Job?.Execute();
                    _jobScheduler.Finish(job);
                    noWorkFound = false;
                    if (profilingEnabled)
                    {
                        currentPerformanceEvent.endTimeStamp = Stopwatch.GetTimestamp();
                        PerformanceEvents.Enqueue(currentPerformanceEvent);
                    }
                }
                else
                {
                    // Try to steal job from different queue
                    for (var i = 0; i < _jobScheduler.Queues.Count; i++)
                    {
                        if (i == WorkerId)
                        {
                            continue;
                        }

                        exists = _jobScheduler.Queues[i].TrySteal(out job);
                        if (!exists)
                        {
                            continue;
                        }

                        if (profilingEnabled)
                        {
                            currentPerformanceEvent = new() { workerId = WorkerId, jobType = job.Job, startTimeStamp = Stopwatch.GetTimestamp() };
                            IsCurrentlyWorking = true;
                        }

                        job.Job?.Execute();
                        _jobScheduler.Finish(job);
                        if (profilingEnabled)
                        {
                            currentPerformanceEvent.endTimeStamp = Stopwatch.GetTimestamp();
                            PerformanceEvents.Enqueue(currentPerformanceEvent);
                        }

                        noWorkFound = false;
                        break;
                    }
                }

                if (noWorkFound)
                {
                    if (profilingEnabled)
                    {
                        IsCurrentlyWorking = false;
                    }

                    _workAvailable.Reset();
                    _workAvailable.Wait(100, token);
                }
            }
        }
        catch (OperationCanceledException)
        {
            //Console.WriteLine("Operation was canceled");
        }
        finally
        {
            //Console.WriteLine("Worker thread is cleaning up and exiting");
        }
    }
}
