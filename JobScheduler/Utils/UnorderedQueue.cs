using System.Collections.Concurrent;

namespace Schedulers.Utils;

internal class UnorderedThreadSafeQueue<T>
{
    private ConcurrentQueue<T> _queue = new();
    private const int maxItems = 512 * 100;

    public UnorderedThreadSafeQueue()
    {
        for (var i = 0; i < maxItems; i++)
        {
            _queue.Enqueue(default!);
        }

        for (var i = 0; i < maxItems; i++)
        {
            if (!_queue.TryDequeue(out _))
            {
                throw new("Failed to dequeue item during init for some reason, this should never happen.");
            }
        }
    }

    internal bool TryDequeue(out T item)
    {
        return _queue.TryDequeue(out item);
    }

    internal bool TryEnqueue(T item)
    {
        if (_queue.Count > maxItems)
        {
            return false;
        }
        _queue.Enqueue(item);
        return true;
    }

    internal void ForceEnqueue(T item)
    {
        _queue.Enqueue(item); }
}
