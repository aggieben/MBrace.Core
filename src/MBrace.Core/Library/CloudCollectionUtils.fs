﻿/// Assortment of common utilities for CloudCollection
module MBrace.Library.CloudCollectionUtils

open System
open System.Collections
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.IO
open System.Runtime.Serialization
open System.Text

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core
open MBrace.Library

/// ICloudCollection wrapper for serializable IEnumerables
[<Sealed; DataContract>]
type SequenceCollection<'T> (seq : seq<'T>) =
    static let getCount (seq : seq<'T>) =
        match seq with
        | :? ('T list) as ts -> ts.Length
        | :? ICollection<'T> as c -> c.Count
        | _ -> Seq.length seq
        |> int64

    static let isKnownCount (seq : seq<'T>) =
        match seq with
        | :? ('T list)
        | :? ICollection<'T> -> true
        | _ -> false

    [<DataMember(Name = "Sequence")>]
    let seq = seq

    /// Gets the underlying IEnumerable
    member __.Sequence = seq

    interface seq<'T> with
        member x.GetEnumerator() = seq.GetEnumerator() :> IEnumerator
        member x.GetEnumerator() = seq.GetEnumerator()

    interface ICloudCollection<'T> with
        member x.IsKnownSize = isKnownCount seq
        member x.IsKnownCount = isKnownCount seq
        member x.IsMaterialized = isKnownCount seq
        member x.GetCountAsync(): Async<int64> = async { return getCount seq }
        member x.GetSizeAsync(): Async<int64> = async { return getCount seq }
        member x.GetEnumerableAsync(): Async<seq<'T>> = async { return seq }

/// CloudCollection implementation consisting of a set of concatenated CloudCollection partitions
[<Sealed; DataContract>]
type ConcatenatedCollection<'T> (partitions : ICloudCollection<'T> []) =
    [<DataMember(Name = "Partitions")>]
    let partitions = partitions

    /// Gets constituent partitions of concatenated collection
    member __.Partitions = partitions

    interface IPartitionedCollection<'T> with
        member x.IsKnownSize = partitions |> Array.forall (fun p -> p.IsKnownSize)
        member x.IsKnownCount = partitions |> Array.forall (fun p -> p.IsKnownCount)
        member x.IsMaterialized = partitions |> Array.forall (fun p -> p.IsMaterialized)
        member x.GetCountAsync(): Async<int64> = async {
            let! counts = partitions |> Seq.map (fun p -> p.GetCountAsync()) |> Async.Parallel
            return Array.sum counts
        }

        member x.GetSizeAsync(): Async<int64> = async {
            let! counts = partitions |> Seq.map (fun p -> p.GetSizeAsync()) |> Async.Parallel
            return Array.sum counts
        }

        member x.GetPartitions(): Async<ICloudCollection<'T> []> = async { return partitions }
        member x.PartitionCount: Async<int> = async { return partitions.Length }
        member x.GetEnumerableAsync(): Async<seq<'T>> = async {
            return seq {
                for p in partitions do
                    let pseq = p.GetEnumerableAsync() |> Async.RunSync
                    yield! pseq
            }
        }


/// Partitionable HTTP line reader implementation
[<Sealed; DataContract>]
type HTTPTextCollection internal (url : string, [<O;D(null:obj)>]?encoding : Encoding, [<O;D(null:obj)>]?range: (int64 * int64)) =

    [<DataMember(Name = "URL")>]
    let url = url
    [<DataMember(Name = "Encoding")>]
    let encoding = encoding
    [<DataMember(Name = "Range")>]
    let range = range

    let getSize () =
        match range with
        | Some (s,e) -> e - s + 1L
        | None ->
            use stream = new SeekableHTTPStream(url)
            stream.GetLength()


    let toEnumerable () =
        let stream = new SeekableHTTPStream(url)
        match range with
        | Some (s,e) -> TextReaders.ReadLinesRanged(stream, max (s - 1L) 0L, e, ?encoding = encoding)
        | None -> TextReaders.ReadLines(stream, ?encoding = encoding)

    interface seq<string> with
        member x.GetEnumerator() = toEnumerable().GetEnumerator() :> IEnumerator
        member x.GetEnumerator() = toEnumerable().GetEnumerator()

    interface ICloudCollection<string> with
        member c.IsKnownCount = false
        member c.IsKnownSize = true
        member c.IsMaterialized = false
        member c.GetCountAsync () = raise <| new NotSupportedException()
        member c.GetSizeAsync () = async { return getSize () }
        member c.GetEnumerableAsync() = async { return toEnumerable () }

    interface IPartitionableCollection<string> with
        member cs.GetPartitions(weights : int []) = async {
            match range with
            // return self if already partitioned
            | Some _ -> return [|cs|]
            | None ->

            let size = getSize ()

            let mkRangedSeqs (weights : int[]) =
                let mkRangedSeq rangeOpt =
                    match rangeOpt with
                    | Some(s,e) when e >= s ->
                        new HTTPTextCollection(url, ?encoding = encoding, ?range = rangeOpt) :> ICloudCollection<string>
                    | _ -> new SequenceCollection<string>([||]) :> _

                let (s, e) = match range with Some (s, e) -> (s, e + 1L) | None -> (0L, size)
                let partitions = Array.splitWeightedRange weights s e
                Array.map mkRangedSeq partitions

            return mkRangedSeqs weights
        }

type CloudCollection private () =

    /// <summary>
    ///     Creates a single-partition CloudCollection instance based on a given sequence.
    /// </summary>
    /// <param name="sequence">Input sequence.</param>
    static member OfSeq(sequence : seq<'T>) =
        new SequenceCollection<'T>(sequence) :> ICloudCollection<'T>


    /// <summary>
    ///     Creates a partitionable CloudCollection instance based on given http url.
    /// </summary>
    /// <param name="url">Url to HTTP resource.</param>
    /// <param name="encoding">Text encoding used for http resource.</param>
    /// <param name="ensureThatFileExists">Ensure that file exists before returning the collection. Defaults to false.</param>
    static member OfHttpFile(url : string, [<O;D(null:obj)>]?encoding : Encoding, [<O;D(null:obj)>]?ensureThatFileExists : bool, ?httpClient : HttpClient) = async {
        if defaultArg ensureThatFileExists false then
            // sanity check; ensure that file exists
            let client = defaultArg httpClient (new HttpClient())
            use request = new HttpRequestMessage(HttpMethod.Head, url)

            let! _ = client.SendAsync(request)
                     |> Async.AwaitTaskCorrect

            match httpClient with
            | Some _ -> ()
            | None -> client.Dispose()

        return HTTPTextCollection(url, ?encoding = encoding)
    }

    /// <summary>
    ///     Concatenates a collection of CloudCollections into a single, partitioned entity.
    /// </summary>
    /// <param name="partitions">Constituent partitions.</param>
    static member Concat(partitions : seq<#ICloudCollection<'T>>) : IPartitionedCollection<'T> =
        let partitions = partitions |> Seq.map (fun p -> p :> ICloudCollection<'T>) |> Seq.toArray
        new ConcatenatedCollection<'T>(partitions) :> IPartitionedCollection<'T>

    /// <summary>
    ///     Traverses provided cloud collections for partitions,
    ///     returning their irreducible components while preserving ordering.
    /// </summary>
    /// <param name="collections">Input cloud collections.</param>
    static member ExtractPartitions (collections : seq<#ICloudCollection<'T>>) : Async<ICloudCollection<'T> []> = async {
        let rec extractCollection (c : ICloudCollection<'T>) : Async<seq<ICloudCollection<'T>>> =
            async {
                match c with
                | :? IPartitionedCollection<'T> as c ->
                    let! partitions = c.GetPartitions()
                    return! extractCollections partitions
                | c -> return Seq.singleton c
            }

        and extractCollections (cs : seq<ICloudCollection<'T>>) : Async<seq<ICloudCollection<'T>>> =
            async {
                let! extracted = cs |> Seq.map extractCollection |> Async.Parallel
                return Seq.concat extracted
            }

        let! extracted = collections |> Seq.map (fun p -> p :> ICloudCollection<'T>) |> extractCollections
        return Seq.toArray extracted
    }

    /// <summary>
    ///     Traverses provided cloud collection for partitions,
    ///     returning its irreducible components while preserving ordering.
    /// </summary>
    /// <param name="collections">Input cloud collections.</param>
    static member ExtractPartitions (collection : ICloudCollection<'T>) : Async<ICloudCollection<'T> []> = CloudCollection.ExtractPartitions([|collection|])

    /// <summary>
    ///     Performs partitioning of provided irreducible CloudCollections to supplied workers.
    ///     This partitioning scheme takes collection sizes as well as worker capacities into account
    ///     in order to achieve uniformity. It also takes IPartitionableCollection (i.e. dynamically partitionable collections)
    ///     into account when traversing.
    /// </summary>
    /// <param name="collections">Collections to be partitioned.</param>
    /// <param name="workers">Workers to partition among.</param>
    /// <param name="isTargetedWorkerEnabled">Enable targeted (i.e. weighted) worker support. Defaults to true.</param>
    /// <param name="weight">Worker weight function. Default to processor count map.</param>
    static member PartitionBySize (collections : ICloudCollection<'T> [], workers : IWorkerRef [], [<O;D(null:obj)>]?isTargetedWorkerEnabled : bool, [<O;D(null:obj)>]?weight : IWorkerRef -> int) = async {
        let weight = defaultArg weight (fun w -> w.ProcessorCount)
        let isTargetedWorkerEnabled = defaultArg isTargetedWorkerEnabled true

        let rec aux (accPartitions : (IWorkerRef * ICloudCollection<'T> []) list)
                    (currWorker : IWorkerRef) (remWorkerSize : int64) (accWorkerCollections : ICloudCollection<'T> list)
                    (remWorkers : (IWorkerRef * int64) list) (remCollections : (ICloudCollection<'T> * int64) list) = async {

            let mkPartition worker (acc : ICloudCollection<'T> list) = worker, acc |> List.rev |> List.toArray

            match remWorkers, accWorkerCollections, remCollections with
            // remaining collection set exhausted, return accumulated partitions with empty sets for remaining workers.
            | _, _, [] ->
                return [|
                    yield! List.rev accPartitions
                    yield mkPartition currWorker accWorkerCollections
                    for rw,_ in remWorkers -> (rw, [||]) |]

            // remaining worker set exhausted, shoehorn all remaining collections to the current worker.
            | [], awc, rcs ->
                let rcs = rcs |> List.map fst |> List.rev
                return! aux accPartitions currWorker 0L (rcs @ awc) [] []

            // next collection is within remaining worker size, include to accumulated collections and update size.
            | _, _, (c, csz) :: rc when csz <= remWorkerSize ->
                return! aux accPartitions currWorker (remWorkerSize - csz) (c :: accWorkerCollections) remWorkers rc

            // next collection is partitionable that does not fit in current worker, begin dynamic partitioning logic.
            | _, _, (:? IPartitionableCollection<'T> as pc, csz) :: rc ->
                // traverse remaining workers, computing size of partitionable allocated to each of them.
                let rec getSizes (acc : (IWorkerRef * int64) list) (workers : (IWorkerRef * int64) list) (remSize : int64) =
                    if remSize <= 0L then List.rev acc, workers else

                    match workers with
                    | [] -> failwith "CloudCollection.PartitionBySize: internal error."
                    | (w, _) :: [] -> getSizes ((w, remSize) :: acc) [(w, 0L)] 0L
                    | (w, wsize) :: rest when wsize >= remSize -> getSizes ((w, remSize) :: acc) ((w, wsize - remSize) :: rest) 0L
                    | (_, wsize) as w :: rest -> getSizes (w :: acc) rest (remSize - wsize)

                let sizes, remWorkers2 = getSizes [] ((currWorker, remWorkerSize) :: remWorkers) csz

                // compute partition weights based on calculated worker sizes

                // normalize weights so that they fit in int32
                let normalize (weights : int64[]) =
                    let rec aux i n =
                        if n > int64 Int32.MaxValue then aux (2L * i) (n / 2L)
                        else i

                    let n = aux 1L (Array.max weights)
                    weights |> Array.map (fun w -> w / n |> int)

                let weights = sizes |> Seq.map snd |> Seq.toArray |> normalize

                // extract partitions based on weights
                let! cpartitions = pc.GetPartitions weights

                // Partition array should contain at least 2 elements:
                // * the first partition is assigned to the current worker.
                // * the last partition will be included in the accumulator state at the tail call.
                // * intermediate partitions, if they exist, will be included as standalone collections
                //   in their assigned workers .

                let firstPartition = mkPartition currWorker (cpartitions.[0] :: accWorkerCollections)
                let lastPartition = cpartitions.[cpartitions.Length - 1]
                let intermediatePartitions =
                    [
                        for i = 1 to cpartitions.Length - 2 do
                            let w,_ = remWorkers.[i - 1]
                            yield (w, [| cpartitions.[i] |])
                    ]

                let newCurrWorker, newCurrSize = List.head remWorkers2
                let remWorkers3 = List.tail remWorkers2
                return! aux (List.rev (firstPartition :: intermediatePartitions) @ accPartitions) newCurrWorker newCurrSize [lastPartition] remWorkers3 rc

            // include if remaining capacity is more than half the partition size
            | (w, wsz) :: rw, _, (c, csz) :: rc when remWorkerSize * 2L > csz ->
                let partition = mkPartition currWorker (c :: accWorkerCollections)
                return! aux (partition :: accPartitions) w wsz [] rw rc

            // include if no other collection has been accumulated in current worker and
            // remaining capacity is more than a third of the partition size
            | (w, wsz) :: rw, [], (c, csz) :: rc when remWorkerSize * 3L > csz ->
                let partition = mkPartition currWorker (c :: accWorkerCollections)
                return! aux (partition :: accPartitions) w wsz [] rw rc

            // move partition to next worker otherwise
            | (w, wsz) :: rw, _, _ ->
                let partition = mkPartition currWorker accWorkerCollections
                return! aux (partition :: accPartitions) w wsz [] rw remCollections
        }

        let isSizeKnown = collections |> Array.forall (fun c -> c.IsKnownSize)
        if not isSizeKnown then
            // size of collections not known a priori, do not take it into account.
            if isTargetedWorkerEnabled then
                return
                    collections
                    |> Array.splitWeighted (workers |> Array.map weight)
                    |> Array.mapi (fun i cs -> workers.[i], cs)
            // partitions according to worker length.
            else
                return
                    collections
                    |> Array.splitByPartitionCount workers.Length
                    |> Array.mapi (fun i cs -> workers.[i], cs)
        else

        // extract nested collections
        let! collections = CloudCollection.ExtractPartitions collections

        if Array.isEmpty collections then return [||]
        elif Array.isEmpty workers then return invalidArg "workers" "must be non-empty." else

        // compute size per collection and allocate expected size per worker according to weight.
        let! wsizes = collections |> Seq.map (fun c -> async { let! sz = c.GetSizeAsync() in return c, sz }) |> Async.Parallel
        let totalSize = wsizes |> Array.sumBy snd
        let coreCount = workers |> Array.sumBy (fun w -> if isTargetedWorkerEnabled then weight w else 1)
        let sizePerCore = totalSize / int64 coreCount
        let rem = ref <| totalSize % int64 coreCount
        let workers =
            [
                for w in workers do
                    let deg = if isTargetedWorkerEnabled then int64 (weight w) else 1L
                    let r = min deg !rem
                    rem := !rem - r
                    let size = deg * sizePerCore + r
                    yield (w, size)
            ]

        match workers with
        | [] -> return invalidArg "workers" "Should be non-empty collection."
        | (hWorker, hSize) :: tailW -> return! aux [] hWorker hSize [] tailW (Array.toList wsizes)
    }

    /// <summary>
    ///     Recursively extracts scheduling information from a set of targeted partition collections.
    /// </summary>
    /// <param name="collections">PartitionedCollections to be extracted.</param>
    static member ExtractTargetedCollections(collections : seq<#ITargetedPartitionCollection<'T>>) : Async<(IWorkerRef * ICloudCollection<'T> []) []> = async {
        let rec extractC (w : IWorkerRef, c : ICloudCollection<'T>) : Async<seq<IWorkerRef * ICloudCollection<'T>>> =
            async {
                match c with
                | :? ITargetedPartitionCollection<'T> as tpc ->
                    let! partitions = tpc.GetTargetedPartitions()
                    return! extractCs partitions

                | :? IPartitionedCollection<'T> as tc ->
                    let! partitions = tc.GetPartitions()
                    return! partitions |> Seq.map (fun p -> (w, p)) |> extractCs

                | c -> return Seq.singleton (w,c)
            }

        and extractCs (cs : seq<IWorkerRef * ICloudCollection<'T>>) : Async<seq<IWorkerRef * ICloudCollection<'T>>> =
            async {
                let! extracted = cs |> Seq.map extractC |> Async.Parallel
                return Seq.concat extracted
            }

        let! partitions = collections |> Seq.map (fun c -> c.GetTargetedPartitions()) |> Async.Parallel
        return
            partitions
            |> Seq.concat
            |> Seq.groupBySequential fst
            |> Array.map (fun (w,ts) -> w, ts |> Array.map snd)
    }