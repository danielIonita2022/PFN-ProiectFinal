using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

internal static class Program
{
    private sealed record Cfg(int Readers, int DurationSeconds, int K, int MaxLen, string OutCsv);

    private sealed record Result(
        string Scenario,
        long Reads,
        long Writes,
        long ElapsedMs,
        long ManagedBefore,
        long ManagedAfter,
        long WsBefore,
        long WsAfter
    )
    {
        public double ReadsPerSec => Reads / (ElapsedMs / 1000.0);
        public double WritesPerSec => Writes / (ElapsedMs / 1000.0);
        public long ManagedDelta => ManagedAfter - ManagedBefore;
        public long WsDelta => WsAfter - WsBefore;
    }

    static int Main(string[] args)
    {
        var cfg = Parse(args);

        Console.WriteLine("=== Concurrency benchmark (C#) - MUTABLE ONLY ===");
        Console.WriteLine($"readers={cfg.Readers} | duration={cfg.DurationSeconds}s | K={cfg.K} | maxLen={cfg.MaxLen}");
        Console.WriteLine($"CSV: {cfg.OutCsv}\n");

        EnsureCsv(cfg.OutCsv);

        var r = RunMutable(cfg);
        Print(r);

        AppendCsv(cfg.OutCsv, "C#", cfg, r);

        Console.WriteLine("\nDone.");
        return 0;
    }

    private static Result RunMutable(Cfg cfg)
    {
        var gate = new object();
        var list = new List<int>(capacity: cfg.MaxLen);
        for (int i = 0; i < Math.Min(cfg.MaxLen, 1024); i++) list.Add(i);

        ForceGc();
        long managedBefore = GC.GetTotalMemory(true);
        long wsBefore = Process.GetCurrentProcess().WorkingSet64;

        long reads = 0, writes = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(cfg.DurationSeconds));
        var start = new ManualResetEventSlim(false);

        // 1 writer
        var writer = Task.Run(() =>
        {
            int x = 0;
            start.Wait();
            while (!cts.IsCancellationRequested)
            {
                lock (gate)
                {
                    if (list.Count >= cfg.MaxLen) list.Clear();
                    list.Add(x++);
                }
                Interlocked.Increment(ref writes);
            }
        });

        // many readers
        Task[] readersTasks = new Task[cfg.Readers];
        for (int i = 0; i < cfg.Readers; i++)
        {
            readersTasks[i] = Task.Run(() =>
            {
                long local = 0;
                start.Wait();
                while (!cts.IsCancellationRequested)
                {
                    int sum = 0;
                    lock (gate)
                    {
                        int take = Math.Min(cfg.K, list.Count);
                        for (int j = 0; j < take; j++)
                            sum += list[list.Count - 1 - j];
                    }
                    if (sum == int.MinValue) Console.WriteLine("impossible");
                    local++;
                }
                Interlocked.Add(ref reads, local);
            });
        }

        var sw = Stopwatch.StartNew();
        start.Set();
        Task.WaitAll(readersTasks);
        writer.Wait();
        sw.Stop();

        ForceGc();
        long managedAfter = GC.GetTotalMemory(true);
        long wsAfter = Process.GetCurrentProcess().WorkingSet64;

        return new Result(
            Scenario: "Mutable List<int> + lock (reads+writes)",
            Reads: reads,
            Writes: writes,
            ElapsedMs: sw.ElapsedMilliseconds,
            ManagedBefore: managedBefore,
            ManagedAfter: managedAfter,
            WsBefore: wsBefore,
            WsAfter: wsAfter
        );
    }

    // ===== CSV =====
    private static void EnsureCsv(string path)
    {
        if (File.Exists(path) && new FileInfo(path).Length > 0) return;

        File.WriteAllText(path,
            "timestamp,language,scenario,readers,seconds,k,maxLen,reads,writes,elapsed_ms,reads_per_sec,writes_per_sec," +
            "managed_before,managed_after,managed_delta,ws_before,ws_after,ws_delta\n");
    }

    private static void AppendCsv(string path, string lang, Cfg cfg, Result r)
    {
        string line = string.Join(",",
            DateTimeOffset.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            lang,
            r.Scenario.Replace(",", " "),
            cfg.Readers.ToString(CultureInfo.InvariantCulture),
            cfg.DurationSeconds.ToString(CultureInfo.InvariantCulture),
            cfg.K.ToString(CultureInfo.InvariantCulture),
            cfg.MaxLen.ToString(CultureInfo.InvariantCulture),
            r.Reads.ToString(CultureInfo.InvariantCulture),
            r.Writes.ToString(CultureInfo.InvariantCulture),
            r.ElapsedMs.ToString(CultureInfo.InvariantCulture),
            r.ReadsPerSec.ToString("0.00", CultureInfo.InvariantCulture),
            r.WritesPerSec.ToString("0.00", CultureInfo.InvariantCulture),
            r.ManagedBefore.ToString(CultureInfo.InvariantCulture),
            r.ManagedAfter.ToString(CultureInfo.InvariantCulture),
            r.ManagedDelta.ToString(CultureInfo.InvariantCulture),
            r.WsBefore.ToString(CultureInfo.InvariantCulture),
            r.WsAfter.ToString(CultureInfo.InvariantCulture),
            r.WsDelta.ToString(CultureInfo.InvariantCulture)
        ) + "\n";

        File.AppendAllText(path, line);
    }

    // ===== console =====
    private static void Print(Result r)
    {
        Console.WriteLine($"--- {r.Scenario} ---");
        Console.WriteLine($"Elapsed: {r.ElapsedMs:n0} ms");
        Console.WriteLine($"Reads:   {r.Reads:n0}  ({r.ReadsPerSec:n0} ops/s)");
        Console.WriteLine($"Writes:  {r.Writes:n0}  ({r.WritesPerSec:n0} ops/s)");
        Console.WriteLine($"GC mem:  {Fmt(r.ManagedBefore)} -> {Fmt(r.ManagedAfter)} (Δ {Fmt(r.ManagedDelta)})");
        Console.WriteLine($"WS mem:  {Fmt(r.WsBefore)} -> {Fmt(r.WsAfter)} (Δ {Fmt(r.WsDelta)})");
    }

    private static string Fmt(long b)
    {
        string[] u = { "B", "KB", "MB", "GB" };
        double x = b; int i = 0;
        while (x >= 1024 && i < u.Length - 1) { x /= 1024.0; i++; }
        return $"{x:0.##} {u[i]}";
    }

    private static void ForceGc()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
    }

    private static Cfg Parse(string[] args)
    {
        int readers = Environment.ProcessorCount;
        int seconds = 10;
        int k = 64;
        int maxLen = 200_000;
        string outCsv = "results_csharp.csv";

        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            string Next() => (i + 1) < args.Length ? args[++i] : throw new ArgumentException($"Missing value after {a}");
            switch (a)
            {
                case "--readers": readers = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "--seconds": seconds = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "--k": k = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "--maxLen": maxLen = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                case "--out": outCsv = Next(); break;
            }
        }

        return new Cfg(readers, seconds, k, maxLen, outCsv);
    }
}
