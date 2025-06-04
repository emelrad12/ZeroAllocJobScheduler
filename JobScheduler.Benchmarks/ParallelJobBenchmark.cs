using System.Diagnostics;
using Arch.Benchmarks;
using Schedulers.Utils;

namespace Schedulers.Benchmarks;

public static class ParallelJobBenchmark
{
    private const int ExpectedCombined = 1_000_000_00;

    private static readonly int[] _innerLoopCount =
    [
        // 200,
        1000,
        // 10_000,
        1_000_000,
        // 1_000_000_00
    ];

    private static int _loopCount;
    private static int _jobCount;

    private static long PJob()
    {
        var timer = Stopwatch.StartNew();
        for (var sindex = 0; sindex < _loopCount; sindex++)
        {
            var job = new ParallelJobProducer<HeavyCalculationJob>(0, _jobCount, new(sindex, sindex));
            job.GetHandle().FlushAndWait();
        }

        return timer.ElapsedTicks;
    }

    private static long PFor()
    {
        var timer = Stopwatch.StartNew();
        for (var sindex = 0; sindex < _loopCount; sindex++)
        {
            Parallel.For(0, _jobCount, i =>
            {
                var job = new HeavyCalculationJob(i, i);
                job.Execute();
            });
        }

        return timer.ElapsedTicks;
    }

    private static long For()
    {
        return 0;
        var timer = Stopwatch.StartNew();
        for (var sindex = 0; sindex < _loopCount; sindex++)
        {
            for (int i = 0; i < _jobCount; i++)
            {
                var job = new HeavyCalculationJob(sindex, sindex);
                job.Execute();
            }
        }

        return timer.ElapsedTicks;
    }

    public static (long, long, long) Run(int style)
    {
        var localExpected = ExpectedCombined;
        _loopCount = localExpected / style;
        _jobCount = localExpected / _loopCount;

        var pJobTime = PJob();
        var pForTime = PFor();
        var forTime = For();

        return (pJobTime, pForTime, forTime);
    }

    public static int GetItersPerSecond(long elapsed)
    {
        return (int)(ExpectedCombined / (elapsed / (double)TimeSpan.TicksPerSecond) / 1_000_000);
    }

    public static void Benchmark()
    {
        ParallelForJobCommon.SetScheduler(new());
        Dictionary<int, (long, long, long)> results = new();
        var totalIters = 0;
        while (true)
        {
            totalIters++;
            foreach (var style in _innerLoopCount)
            {
                var (pJobTime, pForTime, forTime) = Run(style);
                var existing = results.ContainsKey(style);
                if (!existing)
                {
                    results[style] = (pJobTime, pForTime, forTime);
                    Console.WriteLine("$Addding");
                }
                else
                {
                    // Add to existing
                    var data = results[style];
                    data.Item1 += pJobTime;
                    data.Item2 += pForTime;
                    data.Item3 += forTime;
                    results[style] = data;
                }
            }

            foreach (var result in results)
            {
                var style = result.Key;
                var pJob = result.Value.Item1 / totalIters;
                var pFor = result.Value.Item2 / totalIters;
                var forLoop = result.Value.Item3 / totalIters;
                Console.WriteLine($"Style: {style}, PJob: {pJob / TimeSpan.TicksPerMillisecond}ms, PFor: {pFor / TimeSpan.TicksPerMillisecond}ms, For: {forLoop / TimeSpan.TicksPerMillisecond}ms");
                Console.WriteLine($"PJob Iters/sec: {GetItersPerSecond(pJob)}, PFor Iters/sec: {GetItersPerSecond(pFor)}, For Iters/sec: {GetItersPerSecond(forLoop)}");
            }
            Console.WriteLine("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        }

        ParallelForJobCommon.GlobalScheduler.Dispose();
    }
}
