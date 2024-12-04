using System.Diagnostics;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using CommunityToolkit.HighPerformance;
using Schedulers;
using Schedulers.Benchmarks;
using Schedulers.Utils;

namespace Arch.Benchmarks;

public class HeavyCalculationJob : IJob
{
    private double _first;
    private double _second;

    public HeavyCalculationJob(int first, int second)
    {
        _first = first;
        _second = second;
    }

    public void Execute()
    {
        for (var i = 0; i < 50; i++)
        {
            _first = double.Sqrt(_second);
            _second = double.Sqrt(_first) + 1;
        }
    }
}

public class TimerJob
{
    private Stopwatch timer;

    public TimerJob()
    {
        timer = Stopwatch.StartNew();
    }

    public void End(int jobs)
    {
        var time = timer.ElapsedMilliseconds;
        Console.WriteLine($"Job scheduler Time: {time}ms, Jobs: {jobs}, jobs per second {jobs / ((double)time / 1000) / 1_000_000}M");
    }
}

public class Benchmark
{
    private const int jobCount = 2000;
    private const int loopCount = 100;

    private static void BenchB()
    {
        using var jobScheduler = new JobScheduler();
        var timer = new TimerJob();
        for (var sindex = 0; sindex < loopCount; sindex++)
        {
            var parentHandle = new JobHandle();
            Interlocked.Decrement(ref parentHandle._unfinishedJobs);
            for (var index = 0; index < jobCount; index++)
            {
                var job = new HeavyCalculationJob(index, index);
                var handle = jobScheduler.Schedule(job);
                jobScheduler.Flush(handle);
                handle._mainDependency = parentHandle._index;
                Interlocked.Increment(ref parentHandle._unfinishedJobs);
            }

            jobScheduler.Wait(parentHandle);
        }

        timer.End(jobCount * loopCount);
    }

    private static void BenchC()
    {
        var timer = new TimerJob();
        for (var sindex = 0; sindex < loopCount; sindex++)
        {
            var list = new List<HeavyCalculationJob>();
            for (var index = 0; index < jobCount; index++)
            {
                var job = new HeavyCalculationJob(index, index);
                list.Add(job);
            }

            Parallel.ForEach(list, job => job.Execute());
        }

        timer.End(jobCount * loopCount);
    }

    private static void BenchD()
    {
        var timer = new TimerJob();
        for (var sindex = 0; sindex < loopCount; sindex++)
        {
            Parallel.For(0, jobCount, i =>
            {
                var job = new HeavyCalculationJob(i, i);
                job.Execute();
            });
        }

        timer.End(jobCount * loopCount);
    }

    private static void Main(string[] args)
    {
        // var config = DefaultConfig.Instance.AddJob(Job.Default
        //     .WithWarmupCount(2)
        //     .WithMinIterationCount(10)
        //     .WithIterationCount(20)
        //     .WithMaxIterationCount(30)
        //     // .WithAffinity(65535)//To not freeze my pc
        // );
        // config = config.WithOptions(ConfigOptions.DisableOptimizationsValidator);
        // BenchmarkRunner.Run<JobSchedulerBenchmark>(config);
        // return;
        for (int i = 0; i < 10; i++)
        {
            BenchB();
            BenchC();
            BenchD();
        }
        //using var jobScheduler = new JobScheduler();

        // Spawn massive jobs and wait for finish
        /*
        for (var index = 0; index < 1000; index++)
        {
            var indexCopy = index;
            var job = new TestJob(index, () => { Console.WriteLine($"FINISHED {indexCopy}"); });

            var handle1 = jobScheduler.Schedule(job);
            jobScheduler.Flush(handle1);
        }

        Thread.Sleep(10_000);*/

        /*
        var handles = new JobHandle[180];
        for (var index = 0; index < 180; index++)
        {
            var indexCopy = index;
            var job = new TestJob(index, () =>
            {
                //Thread.Sleep(1000);
                Console.WriteLine($"Timeout {indexCopy}");
            });

            var handle = jobScheduler.Schedule(job);
            handle.id = index;
            handles[index] = handle;
        }

        jobScheduler.Flush(handles);
        jobScheduler.Wait(handles);
        Console.WriteLine("Finished");*/

        /*
        var handles = new List<JobHandle>();
        for (var index = 0; index < 180; index++)
        {
            var indexCopy = index;
            var job = new TestJob(index, () =>
            {
                //Thread.Sleep(1000);
                Console.WriteLine($"Timeout {indexCopy}");
            });

            var dependency = new TestJob(index+1000, () =>
            {
                Console.WriteLine($"Timeout {indexCopy+1000}");
            });

            var handle = jobScheduler.Schedule(job);
            handle.id = index;

            var dependencyHandle = jobScheduler.Schedule(dependency);
            dependencyHandle.id = index;

            jobScheduler.AddDependency(dependencyHandle, handle);
            handles.Add(handle);
            handles.Add(dependencyHandle);
        }

        jobScheduler.Flush(handles.AsSpan());
        jobScheduler.Wait(handles.AsSpan());
        Console.WriteLine("Finished");*/

        // Use: dotnet run -c Release --framework net7.0 -- --job short --filter *BenchmarkClass1*
        //BenchmarkSwitcher.FromAssembly(typeof(Benchmark).Assembly).Run(args, config);
    }
}
