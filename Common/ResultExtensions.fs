
namespace AnalysisOfChangeEngine.Common

[<AutoOpen>]
module ResultExtensions =

    [<RequireQualifiedAccess>]
    module Result =

        open System
        open FsToolkit.ErrorHandling


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