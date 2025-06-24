
namespace AnalysisOfChangeEngine.ApiProvider.Excel


[<AutoOpen>]
module Dispatcher =

    open System
    open System.Reflection
    open System.Threading.Tasks
    open System.Threading.Tasks.Dataflow
    open Microsoft.Office.Interop


    type IExcelDispatcher<'TStepRelatedInputs, 'TPolicyRelatedInputs> =
        interface
            inherit IDisposable

            abstract member ExecuteAsync :
                PropertyInfo array
                    -> 'TStepRelatedInputs * 'TPolicyRelatedInputs
                    -> Task<Result<obj array, string>>
        end


    [<NoEquality; NoComparison>]
    type private ExcelCalcRequest<'TStepRelatedInputs, 'TPolicyRelatedInputs> =
        {
            StepRelatedInputs   : 'TStepRelatedInputs
            PolicyRelatedInputs : 'TPolicyRelatedInputs
            RequiredOutputs     : PropertyInfo array
            Callback            : Result<obj array, string> -> unit
        }


    let private bufferBlockOptions =
        // This must remain unbounded or we'll have to rework the
        // calc. submission logic below to be SendAsync instead.
        new DataflowBlockOptions(
            EnsureOrdered = true
        )

    let private actionBlockOptions =
        new ExecutionDataflowBlockOptions(
            // Because each action block corresponds to a single workbook instance.
            MaxDegreeOfParallelism = 1,
            // Do NOT set this to 0; 1 is the minimum. We do this so that a request is always
            // sent to the first available workbook instance.
            BoundedCapacity = 1,
            SingleProducerConstrained = true
        )

    let private linkOptions =
        new DataflowLinkOptions(
            PropagateCompletion = true
        )

    let private createWriter<'TStepRelatedInputs, 'TPolicyRelatedInputs> workbook =
        let stepRelatedInputsWriter =
            DataTransfer<'TStepRelatedInputs>.MakeInputsWriter workbook

        let policyRelatedInputsWriter =
            DataTransfer<'TPolicyRelatedInputs>.MakeInputsWriter workbook

        do workbook.Application.Calculation <-
            Excel.XlCalculation.xlCalculationManual

        fun (stepRelatedInputs, policyRelatedInputs) ->
            do stepRelatedInputsWriter stepRelatedInputs
            do policyRelatedInputsWriter policyRelatedInputs

    let createExcelDispatcher<'TStepRelatedInputs, 'TPolicyRelatedInputs>
        workbookSelector =
            let workbooksFound =
                Locator.locateOpenWorkbooks workbookSelector

            let excelApps =
                workbooksFound
                |> Array.map _.Application

            // For each corresponding application, set the calcualation mode to manual.
            do excelApps
                |> Array.iter (fun app ->
                    do app.Calculation <- Excel.XlCalculation.xlCalculationManual)

            let writers =
                workbooksFound
                |> Array.map createWriter<'TStepRelatedInputs, 'TPolicyRelatedInputs>

            let bufferBlock =
                new BufferBlock<ExcelCalcRequest<'TStepRelatedInputs, 'TPolicyRelatedInputs>>
                    (bufferBlockOptions)

            let dispatchers =
                writers
                |> Array.zip excelApps
                |> Array.map (fun (app, writer) ->
                        let action request =
                            do writer (request.StepRelatedInputs, request.PolicyRelatedInputs)
                            // This will be another, more onerous, blocking action.
                            do app.Calculate ()
                            do request.Callback (Ok Array.empty)

                        new ActionBlock<_> (action, actionBlockOptions)
                    )

            let blockLinks =
                dispatchers
                |> Array.map (fun d -> bufferBlock.LinkTo (d, linkOptions))
        
            {
                new IExcelDispatcher<'TStepRelatedInputs, 'TPolicyRelatedInputs> with
                
                    member _.Dispose (): unit =
                        // The completion will be propogated on account of our link options.
                        do bufferBlock.Complete ()

                        // Sever our links as these will be no longer needed.
                        do blockLinks |> Array.iter _.Dispose()
        
                    member _.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs) =
                        let tcs =
                            new TaskCompletionSource<_> ()  
                            
                        let newCalcRequest =
                            {
                                StepRelatedInputs   = stepRelatedInputs
                                PolicyRelatedInputs = policyRelatedInputs
                                RequiredOutputs     = requiredOutputs
                                Callback            = tcs.SetResult
                            }

                        // This is non-blocking.
                        if not (bufferBlock.Post newCalcRequest) then
                            // Given the buffer block is unbounded, this _should_ never fail.
                            failwith "Unable to submit Excel request."

                        tcs.Task
            }
