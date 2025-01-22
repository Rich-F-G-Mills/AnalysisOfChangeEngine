
namespace rec AnalysisOfChangeEngine.ProviderImplementations

open System
open System.Text.RegularExpressions
open FsToolkit.ErrorHandling


module private Parser =
    let (|ReMatch|_|) pattern =
        let re = new Regex (pattern)

        fun pattern ->
            let _match = re.Match (pattern)

            if _match.Success then
                let groupMatches =
                    _match.Groups
                    |> Seq.cast<Group>
                    |> Seq.skip 1
                    |> Seq.map _.Value
                    |> List.ofSeq

                Some (_match.Value, groupMatches)

            else
                None

    let private (|ColumnRename|_|) =
        (|ReMatch|_|) @"^(.+)->([a-zA-Z][a-zA-Z0-9_]*)$"

    let private (|MemberName|_|) =
        (|ReMatch|_|) @"^([a-zA-Z][a-zA-Z0-9_]*)$"

    let private parseMapping = function
        | ColumnRename (_, [ withinFile; memberName ]) ->
            Ok { WithinFile = withinFile; MemberName = memberName }
        | MemberName (_, [ memberName ]) ->
            Ok { WithinFile = memberName; MemberName = memberName }
        | str ->
            Error (sprintf "Unable to parse column specification '%s'." str)

    let parseSpecification (str: string) =
        result {
            let strSplit =
                str.Replace(Environment.NewLine, "")
                |> _.Split([|','|], StringSplitOptions.None)
                |> Array.map _.Trim()
                |> Array.toList

            let! mappings =
                strSplit
                |> List.map parseMapping
                |> List.sequenceResultM

            do! mappings
                |> Result.requireNotEmpty "At least one column specification must be supplied."

            let countUniqueMemberNames =
                mappings
                |> List.distinctBy _.MemberName
                |> List.length

            do! mappings.Length
                |> Result.requireEqualTo countUniqueMemberNames "Member names must be unique."

            return mappings
        }


// TODO - Needs to be public?
[<NoEquality; NoComparison>]
type public ColumnMapping =
    {
        WithinFile: string
        MemberName: string
    }

    static member public parseSpecification (str) =
        Parser.parseSpecification str
