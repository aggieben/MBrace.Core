﻿namespace Nessos.MBrace

open Nessos.MBrace.Runtime

[<AutoOpen>]
module internal CloudBuilderUtils =

    let inline protect f s = try Choice1Of2 <| f s with e -> Choice2Of2 e

    type Continuation<'T> with
        member inline c.Cancel ctx = c.Cancellation ctx (new System.OperationCanceledException())

        member inline c.Choice (ctx, choice : Choice<'T, exn>) =
            match choice with
            | Choice1Of2 t -> c.Success ctx t
            | Choice2Of2 e -> c.Exception ctx e

        member inline c.Choice (ctx, choice : Choice<Cloud<'T>, exn>) =
            match choice with
            | Choice1Of2 (Body f) -> f ctx c
            | Choice2Of2 e -> c.Exception ctx e

    type ExecutionContext with
        member inline ctx.IsCancellationRequested = ctx.CancellationToken.IsCancellationRequested


    let inline ret t = Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else cont.Success ctx t)
    let inline raiseM<'T> e : Cloud<'T> = Body(fun ctx cont -> if ctx.IsCancellationRequested then cont.Cancel ctx else cont.Exception ctx e)
    let zero = ret ()

    let inline bind (Body f : Cloud<'T>) (g : 'T -> Cloud<'S>) : Cloud<'S> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            let cont' = {
                Success = fun ctx t -> cont.Choice(ctx, protect g t)
                Exception = cont.Exception
                Cancellation = cont.Cancellation
            }

            if Trampoline.IsTrampolineEnabled && Trampoline.LocalInstance.IsBindThresholdReached() then 
                Trampoline.LocalInstance.QueueWorkItem (fun _ -> f ctx cont')
            else
                f ctx cont'
        )

    let inline combine (f : Cloud<unit>) (g : Cloud<'T>) : Cloud<'T> = bind f (fun () -> g)
    let inline delay (f : unit -> Cloud<'T>) : Cloud<'T> = bind zero f

    let inline tryWith (Body f : Cloud<'T>) (handler : exn -> Cloud<'T>) : Cloud<'T> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            let cont' = {
                Success = cont.Success
                Exception = fun ctx e -> cont.Choice(ctx, protect handler e)
                Cancellation = cont.Cancellation
            }

            if Trampoline.IsTrampolineEnabled && Trampoline.LocalInstance.IsBindThresholdReached() then 
                Trampoline.LocalInstance.QueueWorkItem (fun _ -> f ctx cont')
            else
                f ctx cont'
        )

    let inline tryFinally (Body f : Cloud<'T>) (finalizer : unit -> unit) : Cloud<'T> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else
            let cont' = {
                Success = fun ctx t -> match protect finalizer () with Choice1Of2 () -> cont.Success ctx t | Choice2Of2 e -> cont.Exception ctx e
                Exception = fun ctx e -> match protect finalizer () with Choice1Of2 () -> cont.Exception ctx e | Choice2Of2 e' -> cont.Exception ctx e'
                Cancellation = cont.Cancellation
            }

            if Trampoline.IsTrampolineEnabled && Trampoline.LocalInstance.IsBindThresholdReached() then 
                Trampoline.LocalInstance.QueueWorkItem (fun _ -> f ctx cont')
            else
                f ctx cont'
        )

    let inline using<'T, 'S when 'T :> ICloudDisposable> (t : 'T) (g : 'T -> Cloud<'S>) : Cloud<'S> =
        Body(fun ctx cont ->
            if ctx.IsCancellationRequested then cont.Cancel ctx else

            let inline disposer scont =
                let wf = async { return! t.Dispose () }
                Async.StartWithContinuations(wf, scont, cont.Exception ctx, cont.Cancellation ctx, ctx.CancellationToken)

            match protect g t with
            | Choice1Of2 (Body g) ->
                g ctx {
                    Success = fun ctx s -> disposer (fun () -> cont.Success ctx s)
                    Exception = fun ctx e -> disposer (fun () -> cont.Exception ctx e)
                    Cancellation = cont.Cancellation
                }

            | Choice2Of2 e -> disposer (fun () -> cont.Exception ctx e)
        )

    let inline forM (body : 'T -> Cloud<unit>) (ts : 'T []) : Cloud<unit> =
        let rec loop i () =
            if i = ts.Length then zero
            else
                match protect body ts.[i] with
                | Choice1Of2 b -> bind b (loop (i+1))
                | Choice2Of2 e -> raiseM e

        loop 0 ()

    let inline whileM (pred : unit -> bool) (body : Cloud<unit>) : Cloud<unit> =
        let rec loop () =
            match protect pred () with
            | Choice1Of2 true -> bind body loop
            | Choice1Of2 false -> zero
            | Choice2Of2 e -> raiseM e

        loop ()

/// Cloud workflow expression builder
type CloudBuilder () =
    member __.Return (t : 'T) = ret t
    member __.Zero () = zero
    member __.Delay (f : unit -> Cloud<'T>) = delay f
    member __.ReturnFrom (c : Cloud<'T>) = c
    member __.Combine(f : Cloud<unit>, g : Cloud<'T>) = combine f g
    member __.Bind (f : Cloud<'T>, g : 'T -> Cloud<'S>) : Cloud<'S> = bind f g
    member __.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Cloud<'U>) : Cloud<'U> = using value bindF

    member __.TryWith(f : Cloud<'T>, handler : exn -> Cloud<'T>) : Cloud<'T> = tryWith f handler
    member __.TryFinally(f : Cloud<'T>, finalizer : unit -> unit) : Cloud<'T> = tryFinally f finalizer

    member __.For(ts : 'T [], body : 'T -> Cloud<unit>) : Cloud<unit> = forM body ts
    member __.For(ts : seq<'T>, body : 'T -> Cloud<unit>) : Cloud<unit> = forM body (Seq.toArray ts)

    [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 444)>]
    member __.While(pred : unit -> bool, body : Cloud<unit>) : Cloud<unit> = whileM pred body

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CloudBuilder =
        
    /// cloud builder instance
    let cloud = new CloudBuilder ()