using System.Collections.Concurrent;
using Schedulers.Utils;

namespace Schedulers;

public class JobHandleSoaPool
{
    public JobHandleSoaPool()
    {
        const ushort MaxCount = ushort.MaxValue;
        freeIds = new(MaxCount);
        _parent = new ushort[MaxCount];
        _mainDependency = new ushort[MaxCount];
        _dependencies = new List<JobHandle>?[MaxCount];
        _unfinishedJobs = new int[MaxCount];
        _jobs = new IJob[MaxCount];
    }

    public ushort[] _parent;
    public ushort[] _mainDependency;
    public List<JobHandle>?[] _dependencies;
    public int[] _unfinishedJobs;
    public IJob[] _jobs;
    private JobHandlePool freeIds;

    public JobHandle GetNewHandle(IJob iJob)
    {
        freeIds.GetHandle(out var _index);
        return new()
        {
            _index = _index.Value,
            _unfinishedJobs = 1,
            _dependencies = null,
            _parent = ushort.MaxValue,
            _mainDependency = ushort.MaxValue,
            _job = iJob
        };
    }

    public void ReleaseHandle(JobHandle handle)
    {
        freeIds.ReturnHandle(handle);
    }

    public void DecrementUnfinished(ushort jobMainDependency)
    {
        Interlocked.Decrement(ref _unfinishedJobs[jobMainDependency]);
    }
}

/// <summary>
/// The <see cref="JobHandle"/> struct
/// is used to control and await a scheduled <see cref="IJob"/>.
/// <remarks>Size is exactly 64 bytes to fit perfectly into one default sized cacheline to reduce false sharing and be more efficient.</remarks>
/// </summary>
public struct JobHandle
{
    public static JobHandleSoaPool _pool = new();
    public ushort _index;
    public ref IJob _job => ref _pool._jobs[_index];
    public ref ushort _parent => ref _pool._parent[_index];
    public ref ushort _mainDependency => ref _pool._mainDependency[_index];
    public ref List<JobHandle> _dependencies => ref _pool._dependencies[_index];
    public ref int _unfinishedJobs => ref _pool._unfinishedJobs[_index];

    /// <summary>
    /// Creates a new <see cref="JobHandle"/>.
    /// </summary>
    /// <param name="job">The job.</param>
    public JobHandle(IJob job)
    {
        _job = job;
        _parent = ushort.MaxValue;
        _unfinishedJobs = 1;
        _dependencies = null;
    }

    public bool HasDependencies()
    {
        return _dependencies is { Count: > 0 };
    }

    public List<JobHandle> GetDependencies()
    {
        return _dependencies ??= [];
    }

    /// <summary>
    /// Creates a new <see cref="JobHandle"/>.
    /// </summary>
    /// <param name="job">The job.</param>
    /// <param name="parent">Its parent.</param>
    public JobHandle(IJob job, JobHandle parent)
    {
        _job = job;
        _parent = parent._index;
        _unfinishedJobs = 1;
        _dependencies = null;
    }

    public JobHandle(ushort id)
    {
        _index = id;
    }
}
