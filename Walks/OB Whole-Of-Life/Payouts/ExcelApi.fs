
namespace AnalysisOfChangeEngine.Walks.OBWholeOfLife.Payouts


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
                "Excel API [Payouts] [Opening]"

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
                                0.5f
                            OverrideOpeningMonthUnsmoothedReturn =
                                None
                            OverrideClosingMonthUnsmoothedReturn =
                                None
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
                        0.5f
                    OverrideOpeningMonthUnsmoothedReturn =
                        None
                    OverrideClosingMonthUnsmoothedReturn =
                        None
                }

            let executor requiredOutputs (policyRecord, onApiRequestProcessingStart) =
                let policyRelatedInputs =
                    TransferTypes.ExcelPolicyRelatedInputs.ofUnderlying policyRecord                        

                dispatcher.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs) onApiRequestProcessingStart 

            let apiRequestor =
                ApiRequestor.create ("Excel API [Payouts] [Post-Opening Regression]", executor)

            WrappedApiRequestor apiRequestor


    let createClosingMonthEndExcelRequestor
        (dispatcher: IExcelDispatcher<_,_>) (closingRunDate: DateOnly)
        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs> =
            let stepRelatedInputs: TransferTypes.ExcelStepRelatedInputs =
                {
                    OpeningCalculationDate =
                        None
                    ClosingCalculationDate =
                        // Get the last working day of the current run month.
                        closingRunDate.AddMonths(1).AddDays(-1).ToDateTimeMidnight ()
                    FinalMonthInvReturnProportion =
                        0.5f
                    OverrideOpeningMonthUnsmoothedReturn =
                        None
                    OverrideClosingMonthUnsmoothedReturn =
                        None
                }

            let executor requiredOutputs (policyRecord, onApiRequestProcessingStart) =
                let policyRelatedInputs =
                    TransferTypes.ExcelPolicyRelatedInputs.ofUnderlying policyRecord                        

                dispatcher.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs) onApiRequestProcessingStart 

            let apiRequestor =
                ApiRequestor.create ("Excel API [Payouts] [Closing Month-End]", executor)

            WrappedApiRequestor apiRequestor
