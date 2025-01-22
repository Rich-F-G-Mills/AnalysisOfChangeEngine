
namespace AnalysisOfChangeEngine.Implementations

[<AutoOpen>]
module internal ResultExtensions =

    [<RequireQualifiedAccess>]
    module internal Result =

        open System
        open FsToolkit.ErrorHandling


        let inline internal requireThat ([<InlineIfLambda>] predicate) onFail = function
            | Error _ as value -> value
            | Ok value' as value when predicate value' -> value
            | Ok value' -> Error onFail

        let inline private wrapTryParser (parser: string -> (bool * 'T)) (str: string) =
            match parser str with
            | true, value -> Ok value
            | false, _ -> Error str

        let internal makeOptionalParser (parser: string -> Result<'T, string>) =
            fun (str: string) ->
                if String.IsNullOrEmpty str then
                    Ok None
                else
                    parser str |> Result.map Some

        let internal parseInt =
            wrapTryParser Int32.TryParse
        
        let internal parseOptionalInt =
            makeOptionalParser parseInt

        let inline internal parseDateOnly (format: string) (str: string) =
            match DateOnly.TryParseExact (str, format) with
            | true, date -> Ok date
            | false, _ -> Error str

        let inline internal parseOptionalDateOnly (format: string) (str: string) =
            if String.IsNullOrEmpty str then
                Ok None
            else
                parseDateOnly format str
                |> Result.map Some

        let internal parseISODateOnly =
            parseDateOnly "yyyy-MM-dd"

        let internal parseOptionalISODateOnly =
            makeOptionalParser parseISODateOnly

        let internal parseDMYDateOnly =
            parseDateOnly "dd/MM/yyyy"

        let internal parseOptionalDMYDateOnly =
            makeOptionalParser parseDMYDateOnly