using System.Collections.Concurrent;

namespace Schedulers.Utils;

/// <summary>
/// Thread-safe bulk queue implementation.
/// Provides around 10-20% performance in jobs compared to ordinary flush.
/// </summary>
internal class BulkQueue<T>
{
    public BulkQueue(int segmentCount, int segmentSize)
    {
        fullSegments = [];
        emptySegments = [];
        for (var i = 0; i < segmentCount; i++)
        {
            emptySegments.Add(new(segmentSize));
        }
    }

    public struct Segment
    {
        private T[] items;
        private int index;

        public Segment(int capacity)
        {
            items = new T[capacity];
        }

        public bool Dequeue(out T item)
        {
            if (index == 0)
            {
                item = default!;
                return false;
            }

            index--;
            item = items[index];
            items[index] = default!; // Clear the reference
            return true;
        }

        public bool Enqueue(T item)
        {
            if (index >= items.Length)
            {
                return false; // Segment is full
            }

            items[index] = item;
            index++;
            return true;
        }
    }

    private ConcurrentBag<Segment> emptySegments = new();
    private ConcurrentBag<Segment> fullSegments = new();

    public bool GetEmtySegment(out Segment segment)
    {
        return emptySegments.TryTake(out segment);
    }

    public bool GetFullSegment(out Segment segment)
    {
        return fullSegments.TryTake(out segment);
    }

    public void Return(Segment segment)
    {
        emptySegments.Add(segment);
    }

    public void Enqueue(Segment segment)
    {
        fullSegments.Add(segment);
    }
}
