
namespace AnalysisOfChangeEngine.Implementations

[<AutoOpen>]
module rec Types =

    open System


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type ParseOutcome<'T> =
        | Successful of Record: 'T * CleansingChanges: CleansingChange list
        | ParseFailure of ID: string * Reason: string
        | ReadFailure of Reason: string

    [<NoEquality; NoComparison>]
    type CleansingChange =
        {
            Field:  string
            From:   string
            To:     string
            Reason: string
        }

    [<NoEquality; NoComparison>]
    type RunContext =
        {
            RunDate: DateOnly
        }

    type IPolicyRecord =
        interface
            abstract member ID: string with get
        end

    type IPolicyRecordsSource<'T when 'T :> IPolicyRecord> =
        interface
            abstract member IDs: string seq
            abstract member GetDataForIDs: string seq -> Map<string, Result<'T, string>>
        end

