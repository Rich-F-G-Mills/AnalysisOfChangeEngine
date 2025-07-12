
namespace AnalysisOfChangeEngine.Common


[<AutoOpen>]
module TypeExtensions =

    open System
    open System.Collections.Generic


    type IDictionary<'K, 'V> with
        // I was afronted that this method was available for concurrent dictionaries,
        // but not for vanilla ones. Until now!
        member this.GetOrAdd (key: 'K, valueFactory: unit -> 'V) =
            match this.TryGetValue key with
            | true, value ->
                value

            | false, _ ->
                let value =
                    valueFactory ()

                do this.[key] <- value

                value


    let private midnightTimeOnly =
        TimeOnly.FromTimeSpan (TimeSpan.FromSeconds 0)

    type DateOnly with
        // Useful for Excel interop where only DateTimes can (seemingly)
        // be marshalled across.
        member this.ToDateTimeMidnight () =
            this.ToDateTime (midnightTimeOnly)            


    [<RequireQualifiedAccess>]
    module Map =
    
        let inline merge (x: Map<'K, 'V>) (y: Map<'K, 'V>) =
            Map.fold (fun acc key value -> acc |> Map.add key value) x y

        let inline singleton key value =
            Map.ofList [key, value]


    [<RequireQualifiedAccess>]
    module DateOnly =
        
        let inline Min (x: DateOnly, y: DateOnly) =
            if x < y then x else y

        let inline Max (x: DateOnly, y: DateOnly) =
            if x < y then y else x


    [<RequireQualifiedAccess>]
    module Result =

        let inline requireThat ([<InlineIfLambda>] predicate) onFail = function
            | Error _ as value -> value
            | Ok value' as value when predicate value' -> value
            | Ok value' -> Error onFail

        let inline requireSomeWith (error: unit -> 'error) (option: 'ok option) =
            match option with
            | Some x -> Ok x
            | None -> Error (error ())


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