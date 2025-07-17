
namespace AnalysisOfChangeEngine.ApiProvider.Excel


[<AutoOpen>]
module Dispatcher =

    open System
    open System.Collections.Generic
    open System.Reflection
    open System.Threading.Tasks
    open System.Threading.Tasks.Dataflow
    open Microsoft.Office.Interop
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common


    [<NoEquality; NoComparison>]
    type ExcelRequestTelemetry =
        {
            EndpointIdx     : int
            ProcessingStart : DateTime
            ProcessingEnd   : DateTime
        }

    // See the README.md for what's going on here!
    type IExcelDispatcher<'TStepRelatedInputs, 'TPolicyRelatedInputs> =
        interface
            inherit IDisposable

            abstract member ExecuteAsync :
                PropertyInfo array
                    -> 'TStepRelatedInputs * 'TPolicyRelatedInputs
                    -> Task<ApiRequestOutcome * ExcelRequestTelemetry option>
        end

    [<NoEquality; NoComparison>]
    type private ExcelCalcRequest<'TStepRelatedInputs, 'TPolicyRelatedInputs> =
        {
            StepRelatedInputs   : 'TStepRelatedInputs
            PolicyRelatedInputs : 'TPolicyRelatedInputs
            RequiredOutputs     : PropertyInfo array
            Callback            : (ApiRequestOutcome * ExcelRequestTelemetry option) -> unit
        }


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

    // Again, more of a philosophical decision. A dispatcher is tied to a specific
    // set of input types. This is because logic is generated upfront to update the
    // underlying workbook once input instances are supplied for processing.
    /// Creates a load-balancing wrapper around one or more Excel instances. Via the
    /// provided interface, calculation requests can be submitted to the underling Excel
    /// instances.
    let createExcelDispatcher<'TStepRelatedInputs, 'TPolicyRelatedInputs>
        workbookSelector cancellationToken =
            let workbooksFound =
                Locator.locateOpenWorkbooks workbookSelector

            if workbooksFound.Length = 0 then
                failwith "No workbooks found. Please ensure the workbook is open."

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

            let bufferBlockOptions =
                // This must remain unbounded or we'll have to rework the
                // calc. submission logic below to be SendAsync instead.
                new DataflowBlockOptions(
                    EnsureOrdered = true,
                    CancellationToken = cancellationToken
                )

            let actionBlockOptions =
                new ExecutionDataflowBlockOptions(
                    // Because each action block corresponds to a single workbook instance.
                    MaxDegreeOfParallelism = 1,
                    // Do NOT set this to 0; 1 is the minimum. We do this so that a request is always
                    // sent to the first available workbook instance.
                    BoundedCapacity = 1,
                    SingleProducerConstrained = true,
                    CancellationToken = cancellationToken
                )

            let bufferBlock =
                new BufferBlock<ExcelCalcRequest<'TStepRelatedInputs, 'TPolicyRelatedInputs>>
                    (bufferBlockOptions)

            let dispatchers =
                (excelApps, workbooksFound, writers)
                |||> Array.zip3 
                |> Array.mapi (fun idx (app, workbook, writer) ->
                    // Store for later re-use.
                    let endpointId =
                        Some $"{idx}"
                         
                    let cachedRanges =
                        // In theory, this should NEVER be accessed concurrently.
                        new Dictionary<string, Excel.Range> ()

                    let outputReader pi =
                        let excelRangeName =
                            Attributes.getRangeNameFromPI pi

                        // Furthermore, it's not exactly obvious whether this is needed.
                        // Documentation on the RCW suggested the CLR caches wrappers around
                        // COM objects. However, this approach does avoid need to construct
                        // the range object each time.
                        let excelRange =
                            cachedRanges.GetOrAdd
                                (excelRangeName, fun _ ->
                                    workbook.Names[excelRangeName].RefersToRange)

                        let excelValue =
                            excelRange.Value null
                                    
                        // TODO - This isn't particular flexible and can only cope with float32 at
                        // this time. Ideally, we would be able to handle some additional
                        // primitive types (eg. int, bool, ...)
                        match excelValue with
                            | _ when app.WorksheetFunction.IsError excelValue ->
                                Error (ApiRequestFailure.CalculationFailure [| "Value not available." |])

                            | :? float32 when pi.PropertyType = typeof<float32> ->
                                Ok excelValue

                            // In testing, Excel was returning 64-bit float.
                            // Not clear whether this would always be the case though!
                            | :? float as v when pi.PropertyType = typeof<float32> ->
                                // This is nasty. Unbox -> Cast -> Box... Unfortunately,
                                // not obvious there's any other way around this.
                                Ok (upcast float32 v)

                            | _ when pi.PropertyType = typeof<float32> ->
                                Error
                                    (ApiRequestFailure.CalculationFailure
                                        [| "Unable to cast value to float32." |])

                            | _ ->
                                // If we're here, the developer has done something daft and
                                // should be punished. This warrants more than an Error value!
                                failwithf "Unsupported output type '%s'." pi.PropertyType.FullName

                    let action request =
                        let processingStart =
                            DateTime.Now

                        do writer (request.StepRelatedInputs, request.PolicyRelatedInputs)
                        // This will be another, more onerous, blocking action.
                        do app.Calculate ()                                            

                        let outputs =
                            request.RequiredOutputs
                            |> Array.map outputReader
                            |> Array.sequenceResultA
                            |> function
                                Ok results ->
                                    Ok results

                                | Error failures ->
                                    // We need to convert an array of failures into a single
                                    // failure of multiple reasons.
                                    let combinedReasons =
                                        failures
                                        |> Array.collect (function
                                            | ApiRequestFailure.CalculationFailure reasons ->
                                                reasons
                                            | _ ->
                                                // This should not/cannot happen!
                                                failwith "Unexpected failure.")

                                    Error (ApiRequestFailure.CalculationFailure combinedReasons)

                        let processingStart =
                            DateTime.Now

                        let requestTelemetry =
                            {
                                EndpointIdx     = idx
                                ProcessingStart = processingStart
                                ProcessingEnd   = DateTime.Now
                            }

                        do request.Callback (outputs, Some requestTelemetry)

                    new ActionBlock<_> (action, actionBlockOptions))

            let _ =
                dispatchers
                |> Array.map (fun d -> bufferBlock.LinkTo (d, linkOptions))
        
            {
                new IExcelDispatcher<'TStepRelatedInputs, 'TPolicyRelatedInputs> with
                
                    member _.Dispose (): unit =
                        // The completion will be propogated on account of our link options.
                        do bufferBlock.Complete ()

                        // TODO - Should we dispose of the links as well?
        
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

                        // This is non-blocking. Given the buffer block is unbounded,
                        // this _should_ never fail.
                        if not (bufferBlock.Post newCalcRequest) then
                            let timestamp =
                                DateTime.Now

                            tcs.SetResult 
                                (Error
                                    (ApiRequestFailure.CallFailure
                                        [| "Unable to submit Excel request." |]), None)

                        tcs.Task
            }
