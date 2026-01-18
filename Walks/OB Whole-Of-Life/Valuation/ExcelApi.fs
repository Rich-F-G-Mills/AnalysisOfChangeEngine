
namespace AnalysisOfChangeEngine.Walks.OBWholeOfLife.Valuation


[<AutoOpen>]
module ExcelApi =

    open System
    open FsToolkit.ErrorHandling

    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.ApiProvider.Excel
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Walks.OBWholeOfLife


    let createOpeningExcelRequestor
        (dispatcher: IExcelDispatcher<_, _>) (openingRunDate: DateOnly option)
        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs> =
            let requestorName =
                "Excel API [Valuation] [Opening]"

            let apiRequestor =
                match openingRunDate with
                | Some openingRunDate' ->
                    let stepRelatedInputs: TransferTypes.ExcelStepRelatedInputs =
                        {
                            OpeningCalculationDate =
                                None
                            ClosingCalculationDate =
                                openingRunDate'.ToDateTimeMidnight ()
                            FinalMonthInvReturnProportion =
                                1.0f
                            OverrideOpeningMonthUnsmoothedReturn =
                                // We're only pulling out results at the closing position; not used.
                                None
                            OverrideClosingMonthUnsmoothedReturn =
                                Some 0.0f
                        }

                    let executor requiredOutputs (policyRecord, onApiRequestProcessingStart) =
                        let policyRelatedInputs =
                            TransferTypes.ExcelPolicyRelatedInputs.ofUnderlying policyRecord

                        dispatcher.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs) onApiRequestProcessingStart

                    ApiRequestor.create (requestorName, executor)

                | None ->
                    let executor _ _ =
                        failwith "Cannot call the opening requestor when no prior run date is provided."                

                    ApiRequestor.create (requestorName, executor)

            WrappedApiRequestor apiRequestor


    let createPostOpeningExcelRequestor
        (dispatcher: IExcelDispatcher<_,_>) (openingRunDate: DateOnly option, closingRunDate: DateOnly)
        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs> =
            let stepRelatedInputs: TransferTypes.ExcelStepRelatedInputs =
                {
                    OpeningCalculationDate =
                        openingRunDate |> Option.map _.ToDateTimeMidnight()
                    ClosingCalculationDate =
                        closingRunDate.ToDateTimeMidnight ()
                    FinalMonthInvReturnProportion =
                        1.0f
                    OverrideOpeningMonthUnsmoothedReturn =
                        Some 0.0f
                    OverrideClosingMonthUnsmoothedReturn =
                        Some 0.0f
                }

            let executor requiredOutputs (policyRecord, onApiRequestProcessingStart) =
                let policyRelatedInputs =
                    TransferTypes.ExcelPolicyRelatedInputs.ofUnderlying policyRecord                        

                dispatcher.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs) onApiRequestProcessingStart 

            let apiRequestor =
                ApiRequestor.create ("Excel API [Valuation] [Post-Opening Regression]", executor)

            WrappedApiRequestor apiRequestor
