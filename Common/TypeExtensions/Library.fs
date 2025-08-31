
namespace AnalysisOfChangeEngine.Common


/// Not indended for direct developer use.
[<AutoOpen>]
module TypeExtensions =

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Collections.Concurrent


    type Dictionary<'K, 'V> with
        // I was afronted that this method was available for concurrent dictionaries,
        // but not for vanilla ones. Until now!
        // However, need to be careful that this doesn't get used on concurrent dictionaries which
        // inherit from IDictionary. Because of this, I've directly extended Dictionary,
        // rather than IDictionary.
        member this.GetOrAdd (key: 'K, valueFactory: unit -> 'V) =
            match this.TryGetValue key with
            | true, value ->
                value

            | false, _ ->
                let value =
                    valueFactory ()

                do this.[key] <- value

                value

    type ConcurrentDictionary<'K, 'V> with
        
        // Seemed to be needed to convert from F# function to Func<_,_>.
        member this.GetOrAdd (key: 'K, valueFactory: 'K -> 'V) =
            let fn : Func<'K, 'V> =
                valueFactory

            this.GetOrAdd (key, fn)


    let private midnightTimeOnly =
        TimeOnly.FromTimeSpan (TimeSpan.FromSeconds 0)

    type DateOnly with
        // Useful for Excel interop where only DateTimes can (seemingly)
        // be marshalled across.
        member this.ToDateTimeMidnight () =
            this.ToDateTime (midnightTimeOnly)     

    [<RequireQualifiedAccess>]
    module DateOnly =
        
        let inline Min (x: DateOnly, y: DateOnly) =
            if x < y then x else y

        let inline Max (x: DateOnly, y: DateOnly) =
            if x < y then y else x
            

    [<RequireQualifiedAccess>]
    module List =
    
        let inline innerMap ([<InlineIfLambda>] f) xs =
            List.map (List.map f) xs


    [<RequireQualifiedAccess>]
    module Map =
    
        let inline merge (x: Map<'K, 'V>) (y: Map<'K, 'V>) =
            Map.fold (fun acc key value -> acc |> Map.add key value) x y

        let inline singleton key value =
            Map.ofList [key, value]


    [<RequireQualifiedAccess>]
    module Result =

        let inline requireThat ([<InlineIfLambda>] predicate) onFail = function
            | Error _ as value -> value
            | Ok value' as value when predicate value' -> value
            | Ok value' -> Error onFail

        let inline requireSomeWith (onError: unit -> 'error) = function
            | Some x -> Ok x
            | None -> Error (onError ())

        let inline requireNoneWith (onSome: 'T -> 'error) = function
            | Some x -> Error (onSome x)
            | None -> Ok ()


    [<RequireQualifiedAccess>]
    module Option =        
        let requireTrue = function
            | true -> Some ()
            | false -> None

        let requireFalse = function
            | true -> None
            | false -> Some ()


    [<RequireQualifiedAccess>]
    module String =

        // Useful when generating SQL queries.
        let join delim (xs: #seq<string>) =
            String.Join (delim, xs)


    // The following non-empty list logic is a cut-down version of that found in FSharpPlus.
    type NonEmptyList<'T> =
        { Head: 'T; Tail: 'T list }

        interface IEnumerable<'T> with
            member this.GetEnumerator () : IEnumerator<'T> =
                downcast seq {
                    yield this.Head
                    yield! this.Tail
                }

        interface IEnumerable with
            member this.GetEnumerator () =
                upcast (this :> IEnumerable<'T>).GetEnumerator()


    type 'T nonEmptyList =
        NonEmptyList<'T>

    [<RequireQualifiedAccess>]
    module NonEmptyList =
        
        let inline singleton x =
            { Head = x; Tail = List.empty }

        let inline create head tail =
            { Head = head; Tail = tail }

        let inline append x y =
            { Head = x.Head; Tail = x.Tail @ [ y.Head ] @ y.Tail }

        let inline map ([<InlineIfLambda>] f) x =
            { Head = f x.Head; Tail = x.Tail |> List.map f }

        let inline ofList x =
            match x with
            | [] ->
                failwith "Cannot construct from an empty list"
            | x'::xs' ->
                { Head = x'; Tail = xs' }

        let inline ofSeq x =
            // TODO - Not obvious if there is a better way to do this!
            x |> Seq.toList |> ofList


    let inline (.@) x y =
        NonEmptyList.append x y


    type NonEmptyListBuilder () =
        member inline _.Yield (x) =
            NonEmptyList.singleton x
             
        member inline _.YieldFrom (xs: NonEmptyList<_>) =
            xs

        member inline _.Combine (x, y) =
            NonEmptyList.append x (y())

        member inline _.Delay (x: unit -> NonEmptyList<_>) =
            x

        member inline _.Run (x: unit -> _): NonEmptyList<_> =
            x ()

    let nonEmptyList =
        new NonEmptyListBuilder ()
