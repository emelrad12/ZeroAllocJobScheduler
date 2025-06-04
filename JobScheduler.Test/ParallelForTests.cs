using NUnit.Framework;
using Schedulers.Utils;

namespace Schedulers.Test;

[TestFixture]
public class ParallelForTests : IDisposable
{
    public ParallelForTests()
    {
        ParallelForJobCommon.SetScheduler(new());
    }

    public void Dispose()
    {
        ParallelForJobCommon.DisposeScheduler();
    }

    [Test]
    public void TestParallelFor()
    {
        var results = new int[100];
        ParallelForJob.Create(0, results.Length, i =>
        {
            results[i] = i * i;
        }).Flush().Wait();


        for (var i = 0; i < 100; i++)
        {
            Assert.That(results[i], Is.EqualTo(i * i));
        }
    }

    [Test]
    public void TestParallelForStartFromNonZero()
    {
        var results = new int[100];
        ParallelForJob.Create(10, results.Length, i =>
        {
            results[i] = i * i;
        }).Flush().Wait();
        for (var i = 10; i < 100; i++)
        {
            Assert.That(results[i], Is.EqualTo(i * i));
        }
    }

    [Test]
    public void TestParallelForEach()
    {
        var dict = Enumerable.Range(1, 50).ToDictionary(i => i, i => i * 2);
        var sum = 0;

        ParallelForEachJob.Create(dict, kvp =>
        {
            Interlocked.Add(ref sum, kvp.Value);
        }).Flush().Wait();


        Assert.That(sum, Is.EqualTo(dict.Values.Sum()));
    }

    [Test]
    public void TestParallelForEachMultiple()
    {
        for (var j = 0; j < 1; j++)
        {
            var dict = Enumerable.Range(1, 2500).ToDictionary(i => i, i => i * 2);
            var sum = 0;

            ParallelForEachJob.CreateFlushAndWait(dict, kvp =>
            {
                Interlocked.Add(ref sum, kvp.Value);
            });


            Assert.That(sum, Is.EqualTo(dict.Values.Sum()));
        }
        Thread.Sleep(100);
    }

    [Test]
    public void TestLambdaJob()
    {
        var autoresetEvent = new AutoResetEvent(false);
        LambdaJob.CreateFlushAndWait(() =>
        {
            autoresetEvent.Set();
        });
        Assert.That(autoresetEvent.WaitOne(1000), Is.True, "Lambda job did not complete in time.");
    }

    [Test]
    public void NestedJobs()
    {
        var triggers = 0;
        LambdaJob.CreateFlushAndWait(() =>
        {
            ParallelForJob.CreateFlushAndWait(0, 100, i =>
            {
                Interlocked.Increment(ref triggers);
            });
        });
        Assert.That(triggers, Is.EqualTo(100));
    }

    [Test]
    public void UsingSourceJobButNotStarted()
    {
        var triggers = 0;
        var sourceJob = ParallelForJobCommon.GlobalScheduler.Schedule();
        var handle = ParallelForJob.Create(0, 100, i =>
        {
            Interlocked.Increment(ref triggers);
        }, source: sourceJob);
        ParallelForJobCommon.GlobalScheduler.Flush(handle);
        Thread.Sleep(10);
        Assert.That(triggers, Is.EqualTo(0));
    }

    [Test]
    public void UsingSourceJobAndStarted()
    {
        var triggers = 0;
        var sourceJob = ParallelForJobCommon.GlobalScheduler.Schedule();
        var handle = ParallelForJob.Create(0, 100, i =>
        {
            Interlocked.Increment(ref triggers);
        }, source: sourceJob);
        ParallelForJobCommon.GlobalScheduler.Flush(handle);
        ParallelForJobCommon.GlobalScheduler.Flush(sourceJob);
        Thread.Sleep(10);
        Assert.That(triggers, Is.EqualTo(100));
    }
}
