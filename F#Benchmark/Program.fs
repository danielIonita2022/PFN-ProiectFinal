open System
open System.Diagnostics
open System.Globalization
open System.IO
open System.Threading
open System.Threading.Tasks

type Cfg =
  { Readers: int
    Seconds: int
    K: int
    MaxLen: int
    OutCsv: string }

type Result =
  { Scenario: string
    Reads: int64
    Writes: int64
    ElapsedMs: int64
    ManagedBefore: int64
    ManagedAfter: int64
    WsBefore: int64
    WsAfter: int64 }
  member x.ReadsPerSec = float x.Reads / (float x.ElapsedMs / 1000.0)
  member x.WritesPerSec = float x.Writes / (float x.ElapsedMs / 1000.0)
  member x.ManagedDelta = x.ManagedAfter - x.ManagedBefore
  member x.WsDelta = x.WsAfter - x.WsBefore

type Node =
  | Nil
  | Cons of int * Node

let inv = CultureInfo.InvariantCulture

let forceGc () =
  GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true)
  GC.WaitForPendingFinalizers()
  GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true)

let fmtBytes (bytes:int64) =
  let units = [| "B"; "KB"; "MB"; "GB" |]
  let mutable x = float bytes
  let mutable u = 0
  while x >= 1024.0 && u < units.Length - 1 do
    x <- x / 1024.0
    u <- u + 1
  sprintf "%.2f %s" x units[u]

let parse (args:string[]) =
  let mutable cfg =
    { Readers = Environment.ProcessorCount
      Seconds = 10
      K = 64
      MaxLen = 200000
      OutCsv = "results_fsharp.csv" }

  let next i = if i+1 < args.Length then args[i+1] else failwith "missing value"
  let mutable i = 0
  while i < args.Length do
    match args[i] with
    | "--readers" -> cfg <- { cfg with Readers = int (next i) }; i <- i + 2
    | "--seconds" -> cfg <- { cfg with Seconds = int (next i) }; i <- i + 2
    | "--k"       -> cfg <- { cfg with K = int (next i) }; i <- i + 2
    | "--maxLen"  -> cfg <- { cfg with MaxLen = int (next i) }; i <- i + 2
    | "--out"     -> cfg <- { cfg with OutCsv = next i }; i <- i + 2
    | _ -> i <- i + 1
  cfg

let ensureCsv (path:string) =
  if File.Exists(path) && FileInfo(path).Length > 0L then () else
    File.WriteAllText(path,
      "timestamp,language,scenario,readers,seconds,k,maxLen,reads,writes,elapsed_ms,reads_per_sec,writes_per_sec," +
      "managed_before,managed_after,managed_delta,ws_before,ws_after,ws_delta\n"
    )

let appendCsv (path:string) (lang:string) (cfg:Cfg) (r:Result) =
  // IMPORTANT: numeric fields are invariant and WITHOUT thousands separators.
  let line =
    String.Join(",",
      DateTimeOffset.UtcNow.ToString("o", inv),
      lang,
      r.Scenario.Replace(",", " "),
      cfg.Readers.ToString(inv),
      cfg.Seconds.ToString(inv),
      cfg.K.ToString(inv),
      cfg.MaxLen.ToString(inv),
      r.Reads.ToString(inv),
      r.Writes.ToString(inv),
      r.ElapsedMs.ToString(inv),
      r.ReadsPerSec.ToString("0.00", inv),
      r.WritesPerSec.ToString("0.00", inv),
      r.ManagedBefore.ToString(inv),
      r.ManagedAfter.ToString(inv),
      r.ManagedDelta.ToString(inv),
      r.WsBefore.ToString(inv),
      r.WsAfter.ToString(inv),
      r.WsDelta.ToString(inv)
    ) + "\n"
  File.AppendAllText(path, line)

let runPersistentStack (cfg:Cfg) =
  let mutable head = Nil
  let mutable count = 0

  // init small
  for i in 0 .. min 1023 (cfg.MaxLen-1) do
    head <- Cons(i, head)

  forceGc()
  let managedBefore = GC.GetTotalMemory(true) |> int64
  let wsBefore = Process.GetCurrentProcess().WorkingSet64

  let mutable reads = 0L
  let mutable writes = 0L
  use cts = new CancellationTokenSource(TimeSpan.FromSeconds(float cfg.Seconds))
  let start = new ManualResetEventSlim(false)

  // single writer
  let writer =
    Task.Run(fun () ->
      let mutable x = 0
      start.Wait()
      while not cts.IsCancellationRequested do
        // avoid unbounded growth
        let c = Interlocked.Increment(&count)
        if c >= cfg.MaxLen then
          Volatile.Write(&head, Nil)
          Interlocked.Exchange(&count, 0) |> ignore

        // publish new version via CAS
        let mutable done' = false
        while not done' do
          let snap = Volatile.Read(&head)
          let next = Cons(x, snap)
          x <- x + 1
          let prev = Interlocked.CompareExchange(&head, next, snap)
          done' <- Object.ReferenceEquals(prev, snap)

        Interlocked.Increment(&writes) |> ignore
    )

  // many readers (lock-free)
  let readersTasks =
    [| for _ in 1 .. cfg.Readers ->
        Task.Run(fun () ->
          let mutable local = 0L
          start.Wait()
          while not cts.IsCancellationRequested do
            let snap = Volatile.Read(&head)
            let mutable sum = 0
            let mutable taken = 0
            let mutable cur = snap
            while taken < cfg.K do
              match cur with
              | Nil -> taken <- cfg.K
              | Cons(v, nxt) ->
                  sum <- sum + v
                  cur <- nxt
                  taken <- taken + 1
            if sum = Int32.MinValue then Console.WriteLine("impossible")
            local <- local + 1L
          Interlocked.Add(&reads, local) |> ignore
        )
    |]

  let sw = Stopwatch.StartNew()
  start.Set()
  Task.WaitAll(readersTasks)
  writer.Wait()
  sw.Stop()

  forceGc()
  let managedAfter = GC.GetTotalMemory(true) |> int64
  let wsAfter = Process.GetCurrentProcess().WorkingSet64

  { Scenario = "Persistent Node (Cons/Nil) + CAS (lock-free reads)"
    Reads = reads; Writes = writes; ElapsedMs = sw.ElapsedMilliseconds
    ManagedBefore = managedBefore; ManagedAfter = managedAfter
    WsBefore = wsBefore; WsAfter = wsAfter }

[<EntryPoint>]
let main argv =
  let cfg = parse argv

  Console.WriteLine("=== Concurrency benchmark (F#) - IMMUTABLE ONLY ===")
  Console.WriteLine($"readers={cfg.Readers} | duration={cfg.Seconds}s | K={cfg.K} | maxLen={cfg.MaxLen}")
  Console.WriteLine($"CSV: {cfg.OutCsv}\n")

  ensureCsv cfg.OutCsv

  let r = runPersistentStack cfg

  // console pretty (can use thousands separators here)
  Console.WriteLine($"--- {r.Scenario} ---")
  Console.WriteLine($"Elapsed: {r.ElapsedMs:N0} ms")
  Console.WriteLine($"Reads:   {r.Reads:N0}  ({r.ReadsPerSec:N0} ops/s)")
  Console.WriteLine($"Writes:  {r.Writes:N0}  ({r.WritesPerSec:N0} ops/s)")
  Console.WriteLine($"GC mem:  {fmtBytes r.ManagedBefore} -> {fmtBytes r.ManagedAfter} (Δ {fmtBytes r.ManagedDelta})")
  Console.WriteLine($"WS mem:  {fmtBytes r.WsBefore} -> {fmtBytes r.WsAfter} (Δ {fmtBytes r.WsDelta})")

  appendCsv cfg.OutCsv "F#" cfg r
  0
