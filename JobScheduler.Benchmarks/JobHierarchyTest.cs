using Arch.Benchmarks;

namespace Schedulers.Benchmarks;

public class JobHierarchyTest
{
    private class JobThing : IJob
    {
        public void Execute()
        {
            Interlocked.Increment(ref totalPassedJobs);
        }
    }

    private int layers = 6;
    private int growSizePerLayer = 10;
    private int totalScheduledJobs = 0;
    private static int totalPassedJobs = 0;
    private JobScheduler jobScheduler = new();
    private JobThing jobThing = new();

    public JobHierarchyTest()
    {
        // InvertedPyramidJobs();
        // InvertedPyramidJobs();
        // InvertedPyramidJobs();
        // InvertedPyramidJobs();
        for (int i = 0; i < 10; i++)
        {
            PyramidJobs();
        }

        jobScheduler.Dispose();
    }

    private void PyramidJobs()
    {
        totalScheduledJobs = 0;
        totalPassedJobs = 0;
        var timer = new JobTimer();
        var topJob = jobScheduler.Schedule();
        AddPyramidJobs(topJob, 0);
        jobScheduler.Flush(topJob);
        jobScheduler.Wait(topJob);
        timer.End(totalScheduledJobs, "PyramidJobs test");
        var passedExpected = (int)Math.Pow(growSizePerLayer, layers);
        Console.WriteLine($"Total jobs scheduled: {totalScheduledJobs}, total passed jobs: {totalPassedJobs}");
        if (totalPassedJobs != passedExpected)
        {
            throw new($"Total passed jobs {totalPassedJobs} does not match expected {passedExpected}.");
        }
    }

    private void InvertedPyramidJobs()
    {
        totalScheduledJobs = 0;
        totalPassedJobs = 0;
        var timer = new JobTimer();
        var bottomJob = jobScheduler.Schedule();
        var topJob = jobScheduler.Schedule();
        AddInvertedPyramidJobs(bottomJob, 1, topJob);
        jobScheduler.Flush(bottomJob);
        jobScheduler.Flush(topJob);
        jobScheduler.Wait(topJob);
        timer.End(totalScheduledJobs, "InvertedPyramidJobs test");
        var passedExpected = (int)Math.Pow(growSizePerLayer, layers - 1);
        Console.WriteLine($"Total jobs scheduled: {totalScheduledJobs}, total passed jobs: {totalPassedJobs}");
        if (totalPassedJobs != passedExpected)
        {
            throw new($"Total passed jobs {totalPassedJobs} does not match expected {passedExpected}.");
        }
    }

    private void AddPyramidJobs(JobHandle parent, int layer)
    {
        if (layer >= layers)
        {
            return;
        }

        for (var i = 0; i < growSizePerLayer; i++)
        {
            var isNotLastLayer = layer + 1 < layers;
            var handle = jobScheduler.Schedule(isNotLastLayer ? null : jobThing, parent);
            AddPyramidJobs(handle, layer + 1);
            jobScheduler.Flush(handle);
            totalScheduledJobs++;
        }
    }

    private void AddInvertedPyramidJobs(JobHandle source, int layer, JobHandle topJob)
    {
        if (layer >= layers)
        {
            return;
        }

        for (var i = 0; i < growSizePerLayer; i++)
        {
            var isLastLayer = layer + 1 == layers;
            var target = jobScheduler.Schedule(isLastLayer ? jobThing : null);
            target.SetDependsOn(source);
            target.SetParent(topJob);
            AddInvertedPyramidJobs(target, layer + 1, topJob);
            jobScheduler.Flush(target);
            totalScheduledJobs++;
        }
    }
}
