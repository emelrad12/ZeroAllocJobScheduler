namespace Schedulers.Utils;

/// <summary>
/// Job that can be split into multiple jobs.
/// If you set _onlySingle to true, you can ignore RunVectorized
/// If you can make sure your job is always a multiple of the loop size, you can ignore RunSingle
/// </summary>
public interface IParallelJobProducer
{
    /// <summary>
    /// Use this by running a loop
    /// </summary>
    /// <param name="start">Index if the item you should start using</param>
    /// <param name="end">Exclusive end of the range</param>
    public void RunVectorized(int start, int end)
    {
        for (var i = start; i < end; i++)
        {
            RunSingle(i);
        }
    }

    /// <summary>
    /// This is for the remaining items that are not vectorized
    /// </summary>
    /// <param name="index">The current item to be processed</param>
    public void RunSingle(int index)
    {
        throw new NotImplementedException();
    }
}

/// <summary>
/// A job producer that can be used to split a job into multiple jobs.
/// </summary>
/// <typeparam name="T">Type of the </typeparam>
/// <remarks>The reason we pass T instead of just IParallelJobProducer is because the compiler otherwise cannot inline it</remarks>
public struct ParallelJobProducer<T> : IJob where T : IParallelJobProducer
{
    private int _from;
    private readonly int _to;
    private readonly T _producer;
    private readonly JobHandle _selfHandle;
    private readonly JobHandle _sourceHandle;
    private readonly bool _onlySingle;
    private readonly int _loopSize;

    /// <summary>
    /// Creates a new <see cref="ParallelJobProducer{T}"/>.
    /// </summary>
    /// <param name="from">Where the loop starts</param>
    /// <param name="to">Maximum to loop to</param>
    /// <param name="producer">The job to call</param>
    /// <param name="source"></param>
    /// <param name="loopSize">Size of the loop, useful for when you want to use vectorization</param>
    /// <param name="onlySingle">Makes sure you can ignore the end parameter in the IParallelJobProducer, and use start as index</param>
    /// <param name="singleThreaded">Whether the job producer should not spawn children</param>
    public ParallelJobProducer(int from, int to, T producer, int loopSize = 16, bool onlySingle = false, JobHandle source = default, bool singleThreaded = false)
    {
        _sourceHandle = source;
        _from = from;
        _to = to;
        _producer = producer;
        _onlySingle = onlySingle;
        _loopSize = loopSize;
        if (_to - _from == 0)
        {
            throw new ArgumentException($"Invalid range from {_from} to {_to}");
        }
        if (to - from == 1)
        {
            singleThreaded = true; // If the range is only 1, we can just run it single threaded
        }

        _selfHandle = ParallelForJobCommon.GlobalScheduler!.Schedule(singleThreaded ? this : null);
        if (!source.IsNull)
        {
            _selfHandle.SetDependsOn(source);
        }

        if (!singleThreaded)
        {
            CheckAndSplit();
        }
    }

    //Only used to spawn sub-jobs
    private ParallelJobProducer(T producer, JobHandle parent, int start, int end, int loopSize, bool onlySingle, JobHandle source)
    {
        _producer = producer;
        _from = start;
        _to = end;
        if (_to - _from == 0)
        {
            throw new ArgumentException($"Invalid range from {_from} to {_to}");
        }
        _loopSize = loopSize;
        _onlySingle = onlySingle;
        _selfHandle = ParallelForJobCommon.GlobalScheduler!.Schedule(this, parent);
        if (!source.IsNull)
        {
            _selfHandle.SetDependsOn(source);
        }
    }

    /// <summary>
    /// Executes the job.
    /// </summary>
    public void Execute()
    {
        if (_to - _from < 1)
        {
            throw new($"Invalid range from {_from} to {_to}");
        }

        var isSignificantRange = _to - _from > _loopSize * 4;

        // We only split if there is more than one child to split into, otherwise ignore the request
        // Also caching CheckAndSplit is pointless as this should be the last iteration
        if (isSignificantRange && !_onlySingle)
        {
            for (; _from < _to - (_loopSize - 1); _from += _loopSize)
            {
                _producer.RunVectorized(_from, _from + _loopSize);
            }
        }

        for (; _from < _to; _from++)
        {
            _producer.RunSingle(_from);
        }
    }

    /// <summary>
    /// Calculates the amount of children this job will split into.
    /// </summary>
    /// <returns>The amount.</returns>
    private int CalculateChildrenToSplitInto()
    {
        // If you change this also change the bulk queue segment size.
        // This should be equal to the number of threads(or that times 2/3?) but for now it's just a constant
        var ChildrenToSplitInto = 24;
        var range = _to - _from;
        return range < ChildrenToSplitInto ? range : ChildrenToSplitInto;
    }

    /// <summary>
    /// Checks and splits this instance into more instances.
    /// </summary>
    /// <returns>If a split occured</returns>
    private bool CheckAndSplit()
    {
        if (CalculateChildrenToSplitInto() <= 1)
        {
            return false;
        }

        Split();
        return true;
    }

    /// <summary>
    /// Splits this instance into sub instances.
    /// </summary>
    /// <exception cref="Exception">An exception occuring when an invalid range was passed.</exception>
    private void Split()
    {
        if (_selfHandle.IsJobEligibleForExecution())
        {
            throw new InvalidOperationException("Tried to split a job that is already eligible for execution. You should split before flushing the job.");
        }

        var childrenToSplitInto = CalculateChildrenToSplitInto();
        BulkQueue<JobHandle>.Segment segment = default;

        for (var i = 0; i < childrenToSplitInto; i++)
        {
            var start = (int)(_from + (((long)_to - _from) * i / childrenToSplitInto));
            var end = (int)(_from + (((long)_to - _from) * (i + 1) / childrenToSplitInto));
            if (end - start < 1)
            {
                throw new($"Invalid range from {start} to {end}");
            }

            new ParallelJobProducer<T>(_producer, _selfHandle, start, end, _loopSize, _onlySingle, _sourceHandle)._selfHandle.Flush();
        }
    }

    /// <summary>
    /// Returns the <see cref="JobHandle"/>.
    /// </summary>
    /// <returns></returns>
    public JobHandle GetHandle()
    {
        return _selfHandle;
    }
}
