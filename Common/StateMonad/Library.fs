
namespace AnalysisOfChangeEngine.Common.StateMonad


type Stateful<'TState, 'TValue> =
    Stateful of ('TState -> 'TValue * 'TState)


[<RequireQualifiedAccess>]
module Stateful =

    let inline bindM (Stateful transformer) binder =
        Stateful (fun state ->
            let value, newState =
                transformer state

            let (Stateful newTransformer) =
                binder value

            newTransformer newState)

    let inline returnM value =
        Stateful (fun state -> value, state)

    let inline mapM f (Stateful transformer) =
        Stateful (fun state ->
            let value, newState =
                transformer state

            f value, newState)

    let get =
        Stateful (fun state -> state, state)

    let inline put state =
        Stateful (fun _ -> (), state)

    let inline transform f: Stateful<_, unit> =
        Stateful (fun state ->
            let newState =
                f state

            (), newState
        )

    let inline run state (Stateful transformer) =
        transformer state


[<Sealed>]
type StatefulBuilder internal () =
    member _.Zero () =
        Stateful.returnM ()

    member _.Return (x) =
        Stateful.returnM x

    member _.Bind (x, binder) =
        Stateful.bindM x binder

    member _.ReturnFrom (x: Stateful<_, _>) = 
        x

    member _.Delay (x: unit -> Stateful<_, _>) =
        x

    // Needed to combine Zero above... Particularly when raising an exception.
    member _.Combine (Stateful transformer, delayed) =
        Stateful (fun state ->
            let _, newState =
                transformer state

            let (Stateful delayedTransformer) =
                delayed ()

            delayedTransformer newState)

    member _.Run (delayed: unit -> Stateful<_, _>) =
        delayed ()
                

[<AutoOpen>]
module StatefulBuilder =
    let stateful =
        StatefulBuilder ()


[<RequireQualifiedAccess>]
module List =

    // TODO - Could this be made tail-recursive?
    let rec mapStateM f = function
        | [] ->
            Stateful.returnM List.empty
        | x :: xs ->
            Stateful.bindM (f x) (fun x' ->
                Stateful.bindM (mapStateM f xs) (fun xs' ->
                    Stateful.returnM (x' :: xs')))  
