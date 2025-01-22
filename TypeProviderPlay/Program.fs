
namespace AnalysisOfChangeEngine

open FsToolkit.ErrorHandling
open AnalysisOfChangeEngine.TypeProviders


module internal Program =
    type RowSplitter =
        SplitRowType<"ABC,X->DEF,GHI">

    [<EntryPoint>]
    let main _ =
        result {
            let! splitter =
                RowSplitter.CreateSplitter ([| "X"; "ABC"; "GHI" |])

            let! splitRow =
                splitter [| "A"; "B"; "2" |]

            do printfn "%s  %s  %s" splitRow.ABC splitRow.DEF splitRow.GHI

            return 0
        }
        |> Result.defaultWith failwith
    