using System.Diagnostics;
using System.Diagnostics.Contracts;
using Schedulers.Utils;

namespace Schedulers;

/// <summary>
/// The <see cref="JobHandlePool"/> class
/// acts as a pool for meta-data of a <see cref="JobHandle"/> to reduce allocations.
/// </summary>
public class JobHandlePool
{
    /// <summary>
    /// An array that stores the parent of each <see cref="JobHandle"/>.
    /// </summary>
    internal readonly ushort[] Parent;

    /// <summary>
    /// Stores the generation of each <see cref="JobHandle"/>.
    /// It increments each time a <see cref="JobHandle"/> is created.
    /// Technically it is not 100% safe as it could be recycled ushort.MaxValue times, and someone to be still holding a reference to it somehow but that is highly unlikely in practice.
    /// </summary>
    internal readonly ushort[] Generation;

    /// <summary>
    /// An array that stores a list of <see cref="Dependencies"/> of each <see cref="JobHandle"/>.
    /// </summary>
    internal readonly ushort[]?[] Dependencies;

    internal readonly ushort[][] DependenciesBackingArray;

    internal IJob?[] Jobs;
    /// <summary>
    /// An array that stores the unfished jobs of each <see cref="JobHandle"/>-
    /// </summary>
    internal readonly int[] UnfinishedJobs;

    /// <summary>
    /// The <see cref="Utils.JobHandlePool"/> with recycable ids.
    /// </summary>
    private readonly Utils.JobHandlePool _freeIds;

    /// <summary>
    /// Creates a new instance.
    /// </summary>
    internal JobHandlePool()
    {
        // We use a maximum of ushort.MaxValue - 1 because we need to reserve the last id for invalid aka null.
        var MaxCount = ushort.MaxValue - 1;
        _freeIds = new(MaxCount);
        Parent = new ushort[MaxCount];
        Dependencies = new ushort[MaxCount][];
        DependenciesBackingArray = new ushort[MaxCount][];
        const int DependencyCount = 32;
        for (var i = 0; i < MaxCount; i++)
        {
            DependenciesBackingArray[i] = new ushort[DependencyCount];
            for (var j = 0; j < DependencyCount; j++)
            {
                DependenciesBackingArray[i][j] = JobHandle.NullHandleId;
            }
        }

        UnfinishedJobs = new int[MaxCount];
        Generation = new ushort[MaxCount];
        Jobs = new IJob?[MaxCount];
        for (ushort i = 0; i < MaxCount; i++)
        {
            Parent[i] = JobHandle.NullHandleId;
            UnfinishedJobs[i] = 2; // 2 unfinished, because 1 is the job itself, and 1 is whether the job can be scheduled at all yet
        }
    }

    /// <summary>
    /// Rents a new or pooled <see cref="JobHandle"/>.
    /// </summary>
    /// <param name="iJob">The <see cref="IJob"/>.</param>
    /// <returns>A new or pooled <see cref="JobHandle"/> instance.</returns>
    internal JobHandle RentJobHandle(IJob? iJob = null)
    {
        IdWithGeneration index;
        // Wait and hope someone returns a handle
        Stopwatch? stopwatch = null;
        while (!_freeIds.RentHandle(out index))
        {
            if (stopwatch == null)
            {
                stopwatch = Stopwatch.StartNew();
            }

            if (stopwatch.ElapsedMilliseconds > 1000)
            {
                throw new("Possible deadlock detected. No free JobHandles available for more than 1 second. This can happen if you try to execute a graph that is bigger than 32 thousand before starting it. Or maybe there is memory leak somewhere.");
            }

            Thread.Yield();
        }

        var result = new JobHandle(index) { Job = iJob };
        return result;
    }

    /// <summary>
    /// Returns a new or pooled <see cref="JobHandle"/> to the pool.
    /// </summary>
    /// <param name="handle">The <see cref="JobHandle"/>.</param>
    public void ReturnHandle(JobHandle handle)
    {
        // Better do the clearing on the returner thread so we don't have to do it on the main thread.
        if (handle.HasDependents())
        {
            for (var i = 0; i < handle.Dependents!.Length; i++)
            {
                handle.Dependents[i] = JobHandle.NullHandleId;
            }
        }
        // The handle generation is not assigned when the caller uses it, hence it needs to be assigned here.
        handle.Generation = ++Generation[handle.Index];
        handle.Parent = JobHandle.NullHandleId;
        handle.UnfinishedJobs = 2; // 2 unfinished, because 1 is the job itself, and 1 is whether the job can be scheduled at all yet
        handle.Dependents = null;
        handle.Job = null;
        _freeIds.ReturnHandle(handle);
    }
}

/// <summary>
/// The <see cref="JobHandle"/> struct
/// is used to control and await a scheduled <see cref="IJob"/>.
/// <remarks>Size is exactly 64 bytes to fit perfectly into one default sized cacheline to reduce false sharing and be more efficient.</remarks>
/// </summary>
public struct JobHandle
{
    /// <summary>
    /// The <see cref="JobHandlePool"/> for pooling and to reduce allocations.
    /// </summary>
    internal static readonly JobHandlePool Pool = new();

    /// <summary>
    /// Invalid handle value, used to indicate that a <see cref="JobHandle"/> is not valid.
    /// </summary>
    public const ushort NullHandleId = ushort.MaxValue;

    /// <summary>
    /// The index of this <see cref="JobHandle"/>, pointing towards its data inside the <see cref="Pool"/>.
    /// </summary>
    public ushort Index;

    /// <summary>
    /// The generation of the handle.
    /// It is only used to check whether the handle is still valid or has been recycled.
    /// Which is only important when waiting for the handle.
    /// Internally we can safely store handles without generation, as if there is a reference to it, it means it is still valid and has not been recycled yet.
    /// Hence, in that case the generation is irrelevant as it is always the same as the current generation of the pool.
    /// </summary>
    internal ushort Generation;

    private readonly bool _isInitialized;

    /// <summary>
    /// Indicates whether this <see cref="JobHandle"/> is null.
    /// Default or Just JobHandle() is null, meaning it has not been initialized yet.
    /// </summary>
    public readonly bool IsNull
    {
        get => !_isInitialized;
    }

    /// <summary>
    /// Creates a new <see cref="JobHandle"/>.
    /// </summary>
    /// <param name="id">Its id.</param>
    internal JobHandle(IdWithGeneration id)
    {
        Index = id.id;
        Generation = id.generation;
        _isInitialized = true;
    }

    /// <summary>
    /// Creates a new <see cref="JobHandle"/>.
    /// </summary>
    /// <param name="id">Its id.</param>
    public JobHandle(ushort id)
    {
        Index = id;
        _isInitialized = true;
    }

    /// <summary>
    /// Gets a null handle.
    /// </summary>
    public static JobHandle Null
    {
        get => default;
    }

    /// <summary>
    /// The associated <see cref="IJob"/> of this instance.
    /// Can be null if the job is purely used for hierarchy.
    /// </summary>
    public ref IJob? Job
    {
        get => ref Pool.Jobs[Index];
    }

    /// <summary>
    /// The parent of this instance.
    /// </summary>
    public ref ushort Parent
    {
        get => ref Pool.Parent[Index];
    }

    /// <summary>
    /// The dependencies of this instance.
    /// </summary>
    public ref ushort[]? Dependents
    {
        get => ref Pool.Dependencies[Index];
    }

    /// <summary>
    /// Unfinished child jobs, in case that this one is a parent.
    /// </summary>
    public readonly ref int UnfinishedJobs
    {
        get => ref Pool.UnfinishedJobs[Index];
    }

    /// <summary>
    /// Sets the job handle to be ready to execute.
    /// The job still needs to be added to the worker queue to be executed.
    /// It just means that before this point the job cannot be executed.
    /// </summary>
    // The readonly is not really true, but it silences the warning.
    public readonly int SetReadyToExecute()
    {
        var result = Interlocked.Decrement(ref UnfinishedJobs);
        if (result == 0)
        {
            throw new("JobHandle has no unfinished jobs, cannot set it ready to execute.");
        }

        return result;
    }

    /// <summary>
    /// Sets a <see cref="JobHandle"/> that is to be the parent.
    /// If a null handle is passed it will be ignored.
    /// </summary>
    /// <param name="parent">The parent job handle</param>
    public void SetParent(JobHandle parent)
    {
        if (IsJobEligibleForExecution())
        {
            throw new InvalidOperationException("Tried to set a parent on a job that is already eligible for execution. This is not allowed as it can lead to deadlocks or unexpected behavior.");
        }
        if (parent.IsNull)
        {
            throw new ArgumentNullException(nameof(parent), "Cannot set a null parent on a job handle.");
        }

        Interlocked.Increment(ref parent.UnfinishedJobs);
        Parent = parent.Index;
    }

    /// <summary>
    /// Returns whether this instance has <see cref="Dependents"/>.
    /// </summary>
    /// <returns></returns>
    public bool HasDependents()
    {
        return Dependents != null;
    }

    /// <summary>
    /// Returns a list of <see cref="Dependents"/>.
    /// </summary>
    /// <returns></returns>
    public ushort[] GetDependents()
    {
        return Dependents ?? throw new InvalidOperationException();
    }

    /// <summary>
    /// If the <see cref="JobHandle"/> is recycled then it means it did used to have 0 unfinished jobs.
    /// In that case
    /// </summary>
    /// <returns></returns>
    public bool IsFinished()
    {
        return IsHandleAlreadyRecycled() || UnfinishedJobs == 0;
    }

    /// <summary>
    /// Each time a <see cref="JobHandle"/> is created, it gets a generation number. As they are recycled, the generation number increases.
    /// </summary>
    /// <returns>Whether the job handle is currently active or not</returns>
    public bool IsHandleAlreadyRecycled()
    {
        return Generation != Pool.Generation[Index];
    }

    /// <summary>
    /// Sets the <see cref="JobHandle"/> to depend on another to execute before it<see cref="JobHandle"/>.
    /// </summary>
    /// <param name="source">The handle that triggers the current handle</param>
    public void SetDependsOn(JobHandle source)
    {
        if (IsJobEligibleForExecution())
        {
            throw new InvalidOperationException("Tried to set a dependency on a job that is already eligible for execution. This is not allowed as it can lead to deadlocks or unexpected behavior.");
        }
        source.AddDependent(this);
    }

    /// <summary>
    /// A shortcut to <see cref="JobScheduler.Wait(JobHandle)"/>.
    /// Calls the global scheduler wait the job.
    /// In case you want to use other scheduler then use <see cref="JobScheduler.Wait(JobHandle)"/> instead.
    /// </summary>
    public JobHandle Wait()
    {
        ParallelForJobCommon.GlobalScheduler!.Wait(this);
        return this;
    }

    /// <summary>
    /// Flushes the job handle.
    /// </summary>
    public JobHandle Flush()
    {
        ParallelForJobCommon.GlobalScheduler!.Flush(this);
        return this;
    }

    /// <summary>
    /// Combines <see cref="Wait"/> and <see cref="Flush"/>.
    /// </summary>
    public void FlushAndWait()
    {
        Flush();
        Wait();
    }

    /// <summary>
    /// Dependent handles are added to the current <see cref="JobHandle"/> to be executed after it.
    /// If the list of dependents is full, it will act as a linked list and create another node to store the additional dependents.
    /// Subsequent calls to this node will add to the next node in the linked list.
    /// Repeat for as many nodes as needed.
    /// So in theory you can handle unlimited number of dependents.
    /// </summary>
    /// <param name="target">What to be added to be executed after</param>
    private void AddDependent(JobHandle target)
    {
        if (IsJobEligibleForExecution())
        {
            throw new("Tried to add a dependent to a job that is already eligible for execution. This is not allowed as it can lead to deadlocks or unexpected behavior.");
        }
        Dependents ??= Pool.DependenciesBackingArray[Index];
        if (!IsAnySpaceAvailable())
        {
            if (!NextNodeExist())
            {
                var newNextNode = Pool.RentJobHandle();
                // Skip newNextNode.UnfinishedJobs++;
                // As we are going to need to do afterwards newNextNode.UnfinishedJobs--;
                // Because of flushing, hence we just do nothing
                Dependents[^1] = newNextNode.Index;
            }

            var nextNode = new JobHandle(Dependents[^1]);
            nextNode.AddDependent(target);
            return;
        }

        var nextIndex = GetFreeDependentsIndex();
        Interlocked.Increment(ref target.UnfinishedJobs);
        Dependents[nextIndex] = target.Index;
    }

    /// <summary>
    /// Check if the "linked list" has overflowed to a new node.
    /// </summary>
    private bool NextNodeExist()
    {
        if (Dependents == null)
        {
            throw new InvalidOperationException("Dependents are not initialized. Please set them before using this method.");
        }

        return Dependents[^1] != NullHandleId;
    }

    /// <summary>
    /// Checks if the second to last entry in the <see cref="Dependents"/> array is empty.
    /// If it does exist that means there is still space available to add more dependents, in the current node.
    /// </summary>
    private bool IsAnySpaceAvailable()
    {
        if (Dependents == null)
        {
            throw new InvalidOperationException("Dependents are not initialized. Please set them before using this method.");
        }

        return Dependents[^2] == NullHandleId;
    }

    /// <summary>
    /// Returns the index of the first free dependent in the <see cref="Dependents"/> array that can be written to.
    /// </summary>
    private int GetFreeDependentsIndex()
    {
        if (Dependents == null)
        {
            throw new InvalidOperationException("Dependents are not initialized. Please set them before using this method.");
        }

        for (var i = 0; i < Dependents.Length; i++)
        {
            if (Dependents[i] == NullHandleId)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// If the job is eligible for execution, and that means it should be somewhere in the queue waiting to be picked up by a worker.
    /// </summary>
    [Pure]
    public bool IsJobEligibleForExecution()
    {
        return UnfinishedJobs == 1;
    }
}
