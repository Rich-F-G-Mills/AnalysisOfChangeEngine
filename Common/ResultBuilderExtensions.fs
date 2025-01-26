
namespace AnalysisOfChangeEngine.Common


[<AutoOpen>]
module ResultBuilderExtensions =

    open FsToolkit.ErrorHandling


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

