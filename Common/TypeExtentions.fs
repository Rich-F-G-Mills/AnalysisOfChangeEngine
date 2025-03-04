
namespace AnalysisOfChangeEngine

[<AutoOpen>]
module TypeExtensions =

    open System
    open FsToolkit.ErrorHandling


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

        let inline private wrapTryParser (parser: string -> (bool * 'T)) (str: string) =
            match parser str with
            | true, value -> Ok value
            | false, _ -> Error str

        let makeOptionalParser (parser: string -> Result<'T, string>) =
            fun (str: string) ->
                if String.IsNullOrEmpty str then
                    Ok None
                else
                    parser str |> Result.map Some

        let parseInt =
            wrapTryParser Int32.TryParse
        
        let parseOptionalInt =
            makeOptionalParser parseInt

        let parseReal =
            wrapTryParser Double.TryParse

        let parseOptionalReal =
            makeOptionalParser parseReal

        let inline parseDateOnly (format: string) (str: string) =
            match DateOnly.TryParseExact (str, format) with
            | true, date -> Ok date
            | false, _ -> Error str

        let inline parseOptionalDateOnly (format: string) (str: string) =
            if String.IsNullOrEmpty str then
                Ok None
            else
                parseDateOnly format str
                |> Result.map Some

        let parseISODateOnly =
            parseDateOnly "yyyy-MM-dd"

        let parseOptionalISODateOnly =
            makeOptionalParser parseISODateOnly

        let parseDMYDateOnly =
            parseDateOnly "dd/MM/yyyy"

        let parseOptionalDMYDateOnly =
            makeOptionalParser parseDMYDateOnly


    type ResultBuilder with
        member inline _.MergeSources (x1, x2) =
            match x1, x2 with
            | Ok x1', Ok x2' -> Ok (x1', x2')
            | Error err, _
            | _, Error err   -> Error err        

        member inline _.MergeSources3 (x1, x2, x3) =
            match x1, x2, x3 with
            | Ok x1', Ok x2', Ok x3' -> Ok (x1', x2', x3')
            | Error err, _, _
            | _, Error err, _
            | _, _, Error err        -> Error err

        member inline _.MergeSources4 (x1, x2, x3, x4) =
            match x1, x2, x3, x4 with
            | Ok x1', Ok x2', Ok x3', Ok x4' -> Ok (x1', x2', x3', x4')
            | Error err, _, _, _
            | _, Error err, _, _
            | _, _, Error err, _
            | _, _, _, Error err             -> Error err

        member inline _.MergeSources5 (x1, x2, x3, x4, x5) =
            match x1, x2, x3, x4, x5 with
            | Ok x1', Ok x2', Ok x3', Ok x4', Ok x5' -> Ok (x1', x2', x3', x4', x5')
            | Error err, _, _, _, _
            | _, Error err, _, _, _
            | _, _, Error err, _, _
            | _, _, _, Error err, _
            | _, _, _, _, Error err                  -> Error err

