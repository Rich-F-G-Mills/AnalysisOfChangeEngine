
namespace AnalysisOfChangeEngine.Controller


[<AutoOpen>]
module Evaluator =

    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller.WalkAnalyser


    let createEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord : equality>
        (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        (apiCollection: 'TApiCollection) =
            
            let parsedWalk =
                WalkParser.execute walk apiCollection

            0

