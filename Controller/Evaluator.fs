
namespace AnalysisOfChangeEngine.Controller


[<AutoOpen>]
module Evaluator =

    open System.Threading.Tasks
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.Controller.WalkAnalyser


    // We need to at least give a type hint for the abstract requestor, otherwise
    // we cannot access the 'Name' property.
    let private apiResponsesConsolidator (acc: Result<Map<AbstractApiRequestor<_>, _>, _>) x =
        match acc, x with
        // No prior failures and current requestor was successful.
        | Ok acc', (requestor, Ok results) ->
            Ok (acc' |> Map.add requestor results)
        // If we've already encountered a failure, we don't care
        // about any subsequent successes. ie... We're doomed to
        // failure at this point!
        | Error _, (_, Ok _) ->
            acc
        | Ok _, (requestor, Error (ApiRequestFailure.CalculationFailure reasons)) ->
            Error [ EvaluationFailure.ApiCalculationFailure (requestor.Name, reasons) ]
        | Ok _, (requestor, Error (ApiRequestFailure.CallFailure reasons)) ->
            Error [ EvaluationFailure.ApiCallFailure (requestor.Name, reasons) ]
        | Error failures, (requestor, Error (ApiRequestFailure.CalculationFailure reasons)) ->
            Error ((EvaluationFailure.ApiCalculationFailure (requestor.Name, reasons))::failures)
        | Error failures, (requestor, Error (ApiRequestFailure.CallFailure reasons)) ->
            Error ((EvaluationFailure.ApiCallFailure (requestor.Name, reasons))::failures)


    let createEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord : equality>
        (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        (apiCollection: 'TApiCollection) =
            
            let parsedWalk =
                WalkParser.execute walk apiCollection

            let exitedPolicyExecutor: _ -> Task<ExitedPolicyOutcome<'TStepResults>> =
                let openingReRunStepHdr, openingReRunParsedStep =
                    // This CANNOT fail! If it does, I've not idea
                    // how the logic managed to get this far!
                    List.head parsedWalk.ParsedSteps

                assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> openingReRunStepHdr

                let apiCalls, invoker =
                    openingReRunParsedStep.ApiCalls, openingReRunParsedStep.WrappedInvoker           

                let groupedDependenciesByRequestor =
                    apiCalls
                    |> Seq.groupBy _.Requestor
                    |> Seq.map (fun (requestor, dependencies) ->
                        requestor, Seq.toArray dependencies)
                    |> Map.ofSeq
                
                let nestedApiCallTasksByRequestor =
                    groupedDependenciesByRequestor
                    |> Map.map (fun requestor dependencies ->
                        let outputs =
                            dependencies
                            |> Array.map _.OutputProperty

                        let asyncExecutor =
                            requestor.ExecuteAsync outputs

                        asyncExecutor)

                let resultMappers =
                    apiCalls
                    |> Seq.map (fun dependency ->
                        let nestedIdxWithinRequestor =
                            groupedDependenciesByRequestor[dependency.Requestor]
                            |> Array.findIndex _.Equals(dependency)

                        fun (apiResponses: Map<_, obj array>) ->
                            let responsesWithinRequestor =
                                apiResponses[dependency.Requestor]

                            responsesWithinRequestor[nestedIdxWithinRequestor])
                    |> Seq.toArray

                let responseArrayHydrator responsesByRequetor idx =
                    resultMappers[idx] responsesByRequetor

                fun (ExitedPolicy policyRecord) ->
                    backgroundTask {
                        let responseTasksByRequestor =
                            nestedApiCallTasksByRequestor
                            |> Map.map (fun _ asyncExecutor -> asyncExecutor policyRecord)
                            
                        // Allows us to asynchronously await all tasks without using
                        // a blocking Wait command.
                        let! _ =
                            responseTasksByRequestor
                            |> Map.values
                            |> Task.WhenAll

                        // TODO - For now, we'll assume that all tasks complete in
                        // a non-cancelled, non-faulted state.

                        let responsesByRequestor =
                            responseTasksByRequestor
                            |> Map.map (fun _ -> _.Result)

                        let consolidatedResponses =
                            responsesByRequestor
                            |> Map.toSeq
                            |> Seq.fold apiResponsesConsolidator (Ok Map.empty)  
                            
                        let evaluationOutcome : ExitedPolicyOutcome<_> =
                            match consolidatedResponses with
                            | Ok resultsByRequestor ->
                                let apiResults =
                                    Array.init apiCalls.Count (responseArrayHydrator resultsByRequestor)

                                Ok (invoker (policyRecord, apiResults))

                            | Error failures ->
                                // Necessary as we're mapping from one result domain to another.
                                Error failures

                        return evaluationOutcome
                    }

            exitedPolicyExecutor
                
                

