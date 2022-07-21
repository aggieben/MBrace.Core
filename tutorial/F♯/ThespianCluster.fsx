#load "../../.paket/load/net6.0/main.group.fsx"

#r "../../src/MBrace.Core/bin/Debug/net6/MBrace.Core.dll"
#r "../../src/MBrace.Flow/bin/Debug/net6/MBrace.Flow.dll"
#r "../../src/MBrace.Runtime/bin/Debug/net6/MBrace.Runtime.dll"
#r "../../src/MBrace.Thespian/bin/Debug/net6/MBrace.Thespian.dll"


namespace global

module Config =

    open MBrace.Core
    open MBrace.Runtime
    open MBrace.Thespian

    // change to alter cluster size
    let private workerCount = 4

    let mutable private thespian = None

    do
        ThespianWorker.LocalExecutable <- (__SOURCE_DIRECTORY__ + "/../../src/MBrace.Thespian.Worker/bin/Debug/net6/mbrace.thespian.worker.exe")

    /// Gets or creates a new Thespian cluster session.
    let GetCluster() =
        match thespian with
        | None ->
            let cluster =
                ThespianCluster.InitOnCurrentMachine(workerCount,
                                                     logger = new ConsoleLogger(),
                                                     logLevel = LogLevel.Info)
            printfn "got cluster: %A" cluster
            thespian <- Some cluster
        | Some t -> ()
        thespian.Value

    /// Kills the current cluster session
    let KillCluster() =
        match thespian with
        | None -> ()
        | Some t -> t.KillAllWorkers() ; thespian <- None