
namespace AnalysisOfChangeEngine

[<AutoOpen>]
module RegEx =

    open System.Text.RegularExpressions


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