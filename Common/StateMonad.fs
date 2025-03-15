
namespace AnalysisOfChangeEngine.StateMonad

type State<'TState, 'TValue> =
    State of ('TState -> 'TValue * 'TState)

module State =

    let inline bindM (State transformer) binder =
        State (fun state ->
            let value, newState =
                transformer state

            let (State newTransformer) =
                binder value

            newTransformer newState)

    let inline returnM value =
        State (fun state -> value, state)

    let inline mapM f (State transformer) =
        State (fun state ->
            let value, newState =
                transformer state

            f value, newState)

    let get =
        State (fun state -> state, state)

    let inline put state =
        State (fun _ -> (), state)

    let inline run state (State transformer) =
        transformer state


[<Sealed>]
type StateBuilder internal () =
    member _.Zero () =
        State.returnM ()

    member _.Return (x) =
        State.returnM x

    member _.Bind (x, binder) =
        State.bindM x binder

    member _.ReturnFrom (x: State<_, _>) = 
        x

    member _.Delay (x: unit -> State<_, _>) =
        x

    // Needed to combine Zero above... Particularly when raising an exception.
    member _.Combine (State transformer, delayed) =
        State (fun state ->
            let _, newState =
                transformer state

            let (State delayedTransformer) =
                delayed ()

            delayedTransformer newState)

    member _.Run (delayed) =
        delayed ()
                

[<AutoOpen>]
module StateBuilder =
    let state =
        StateBuilder ()


[<RequireQualifiedAccess>]
module List =

    (*
    Unfortunately, Copilot's implementation of List.mapM is not tail-recursive.
    
    [<TailCall>]
    let rec private mapStateM_inner f agg = function
        | [] ->
            State.returnM (List.rev agg)
        | x :: xs ->
            State.bindM (f x) (fun x' ->
                mapStateM_inner f (x' :: agg) xs)

    let mapStateM f =
        mapStateM_inner f []
    *)

    // TODO - Could this be made tail-recursive?
    let rec mapStateM f = function
        | [] ->
            State.returnM List.empty
        | x :: xs ->
            State.bindM (f x) (fun x' ->
                State.bindM (mapStateM f xs) (fun xs' ->
                    State.returnM (x' :: xs')))  
            