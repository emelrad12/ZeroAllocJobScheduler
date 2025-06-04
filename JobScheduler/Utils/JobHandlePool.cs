using System.Collections.Concurrent;

namespace Schedulers.Utils;

internal struct IdWithGeneration
{
    public ushort id;
    public ushort generation;
}

/// <summary>
/// This <see cref="JobHandlePool"/> class
/// acts as a pool for <see cref="JobHandle"/> ids.
/// </summary>
internal class JobHandlePool
{
    // Handles are returned to the _freeHandles queue.
    // Then a thread if free goes and optimizes the handles, by queueuing them into _handleCache.
    // Then the main thread can swap the _handleMain and _handleCache queues.
    private readonly ConcurrentQueue<IdWithGeneration> _mainThreadFreeHandles;
    private Queue<IdWithGeneration> _handleMain;
    private Queue<IdWithGeneration> _handleCache;
    private static readonly int _mainThreadId = Environment.CurrentManagedThreadId;

    // Allocate part of the handles for the main thread and the other part for other threads.
    // This way the main thread can consume handles without any synchronization overhead.
    private readonly ConcurrentQueue<IdWithGeneration> _freeHandlesOtherThreads;
    private readonly ushort _mainThreadHandleCutoff;

    private void SwapQueues()
    {
        lock (this)
        {
            (_handleMain, _handleCache) = (_handleCache, _handleMain);
        }
    }

    /// <summary>
    /// Creates a new instance.
    /// </summary>
    /// <param name="size">Its maximum size.</param>
    public JobHandlePool(int size)
    {
        _mainThreadHandleCutoff = (ushort)(size * 0.5f); // 80% of the handles are for the main thread.
        _mainThreadFreeHandles = new();
        _freeHandlesOtherThreads = new();
        _handleCache = new(_mainThreadHandleCutoff);
        _handleMain = new(_mainThreadHandleCutoff);
        for (ushort handleId = 0; handleId < size; handleId++)
        {
            var newItem = new IdWithGeneration { id = handleId, generation = 0 };
            if (handleId < _mainThreadHandleCutoff)
            {
                _handleMain.Enqueue(newItem);
            }
            else
            {
                _freeHandlesOtherThreads.Enqueue(newItem);
            }
        }
    }

    /// <summary>
    /// Refill the cache with new handles.
    /// </summary>
    private void RechargeCache()
    {
        lock (this)
        {
            while (_mainThreadFreeHandles.TryDequeue(out var handle))
            {
                _handleCache.Enqueue(handle);
            }
        }
    }

    public void TryOptimizeHandles()
    {
        // Random number tuning, but seems like around 3-5 is the sweet spot
        if (_mainThreadFreeHandles.Count > 4)
        {
            RechargeCache();
        }
    }

    /// <summary>
    /// Rents a new handle.
    /// </summary>
    /// <param name="handle">The rented handle.</param>
    /// <remarks>Not intrinsically thread safe so we lock. Assumption is that the user won't generate handles on other threads</remarks>
    /// <returns>Whether there is a handle available</returns>
    internal bool RentHandle(out IdWithGeneration handle)
    {
        // This whole contraption is to allow the main thread to do as little work as possible.
        // This check is free according to benchmarks.
        // If not on the main thread then we use separate pool.
        if (_mainThreadId != Environment.CurrentManagedThreadId)
        {
            return _freeHandlesOtherThreads.TryDequeue(out handle);
        }

        if (_handleMain.Count == 0)
        {
            SwapQueues();
        }

        var result = _handleMain.TryDequeue(out handle);
        return result;
    }

    /// <summary>
    /// Returns a rented handle.
    /// </summary>
    /// <param name="handle">The initial rented handle.</param>
    /// <remarks>Thread safe.</remarks>
    internal void ReturnHandle(JobHandle handle)
    {
        var handleId = handle.Index;
        var newItem = new IdWithGeneration { id = handleId, generation = handle.Generation };
        if (handleId < _mainThreadHandleCutoff)
        {
            _mainThreadFreeHandles.Enqueue(newItem);
            TryOptimizeHandles();
        }
        else
        {
            _freeHandlesOtherThreads.Enqueue(newItem);
        }
    }
}
