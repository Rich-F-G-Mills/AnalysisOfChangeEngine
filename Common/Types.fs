﻿
namespace AnalysisOfChangeEngine


[<AutoOpen>]
module Types =

    open System
    open System.Collections.Generic
    open System.Reflection
    open FSharp.Quotations


    type PolicyID = string
    type Reason = string

    type IPolicyRecord =
        interface
            abstract member ID: PolicyID with get
        end


    [<NoEquality; NoComparison>]
    type RunContext =
        {
            OpeningRunDate: DateOnly
            ClosingRunDate: DateOnly
        }


    type ApiRequestor<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        Map<string, PropertyInfo> -> 'TPolicyRecord -> Result<Map<string, obj>, string>

    type IApiRequestor<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        interface
            abstract Name: string
            abstract Requestor: ApiRequestor<'TPolicyRecord>
        end

    // DD - We also need to track the possible API responses... We cannot use
    // a vanilla type alias as it will complain about TResponse not actually getting used.
    // However, we have no such issue using a DU. Ultimately, this is just an IApiRequestor
    // with some additional type information.
    [<NoEquality; NoComparison>]
    type WrappedApiRequestor<'TPolicyRecord, 'TResponse when 'TPolicyRecord :> IPolicyRecord> =
        | WrappedApiRequestor of IApiRequestor<'TPolicyRecord>

        interface IApiRequestor<'TPolicyRecord> with
            member this.Name =
                match this with
                | WrappedApiRequestor inner ->
                    inner.Name

            member this.Requestor =
                match this with
                | WrappedApiRequestor inner ->
                    inner.Requestor
            

    type ILogger =
        interface
            abstract member LogInfo: message: string -> unit
            abstract member LogDebug: message: string -> unit
            abstract member LogWarning: message: string -> unit
            abstract member LogError: message: string -> unit
        end


    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type StepValidationIssue =
        | Warning of string
        | Error of string


    [<AbstractClass>]
    type SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> private () =
        /// Request specific output from the referenced API.
        // DD - Could have used curried call rather than tupled; however, deciphering it
        // from the resulting call from code quotation is a lot more complicated.
        abstract member apiCall<'TResponse, 'T>
            : apiRequest: ('TApiCollection -> WrappedApiRequestor<'TPolicyRecord, 'TResponse>) * selector: ('TResponse -> 'T) -> 'T


    type SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        // DD - If we supply 'from' and 'apis' as tupled arguments, the resulting quotation
        // is more cumbersome to process. Using curried form makes them easier to identify.
        Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
                -> 'TPolicyRecord
                -> 'TStepResults    // Prior
                -> 'TStepResults    // Current
                -> 'TStepResults>

    module SourceExpr =
        let castExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> expr
            : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                Expr.Cast<_> expr

        let usePrior<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
            : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                <@ fun _ _ prior _ -> prior @>


    // The only items available for validation will be the record itself and the corresponding results.
    type OpeningStepValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    // We have both before and after data along with corresponding step results.
    type DataChangeStepValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    type SourceChangeStepValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationIssue list

    // Similar situation to the open step validator above.
    type AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list


    let noValidator _ : StepValidationIssue list =
        List.empty


    // When given the opening data, immediately prior data and the closing, this
    // logic can optionally provide a modified policy record.
    // The machinery will not check to see if anything has changed.
    // If the user implementation returns anything but None, it is assumed a
    // change has occured.
    // It is assumed that logic cannot be put in a situation where it is NOT
    // possible to provide revised data.
    type PolicyRecordChanger<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        // Opening * Prior * Closing -> Optional Revised
        'TPolicyRecord * 'TPolicyRecord * 'TPolicyRecord -> 'TPolicyRecord option


    type IStepHeader =
        interface
            abstract member Uid         : Guid with get
            abstract member Title       : string with get
            abstract member Description : string with get
        end

    type ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        interface
            inherit IStepHeader
            abstract member Source: SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> with get
        end

    // Applied to steps where policy record fields may change.
    // Does NOT relate to whether individual records themselves are included/excluded.
    type IDataChangeStep<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        interface
            inherit IStepHeader
            abstract member DataChanger: PolicyRecordChanger<'TPolicyRecord> with get
        end

    // The opening data will be specified outside of this. We need only worry
    // about validation.
    [<NoEquality; NoComparison>]
    type OpeningStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            Validator       : OpeningStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description

    // We'd usually run a regression test because of a change in parameterisation
    // of the underlying API. As such, no data changes are expected (nor permitted)
    // for these.
    [<NoEquality; NoComparison>]
    type SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            Source          : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator       : SourceChangeStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description    

        interface ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with
            member this.Source =
                this.Source
                

    // Intended this would be used as part of an opening data restatement along
    // will subsequent data changes in order to walk to the closing data.
    [<NoEquality; NoComparison>]
    type DataChangeStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            DataChanger     : PolicyRecordChanger<'TPolicyRecord>
            Validator       : DataChangeStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description 

        interface IDataChangeStep<'TPolicyRecord> with
            member this.DataChanger = this.DataChanger                


    // No data changer or validator needed here.
    [<NoEquality; NoComparison>]
    type RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description 


    // This a catch-all to ensure that there are no outstanding changes in order
    // to reach the closing data. Given we're only changing the underlying policy
    // data, no change in underlying API expected and validator signature matches
    // this reasoning.
    [<NoEquality; NoComparison>]
    type ClosingExistingDataStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            Validator       : DataChangeStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description

        interface IDataChangeStep<'TPolicyRecord> with
            member _.DataChanger =
                fun (_, _, closing) ->
                    Some closing

    // Again, no change in data or API allowed. All we're doing is
    // changing the filter applied as to whether a given record is
    // considered for processing.
    [<NoEquality; NoComparison>]
    type AddNewRecordsStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            Validator       : AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description


    [<AbstractClass>]
    type AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection  when 'TPolicyRecord :> IPolicyRecord and 'TPolicyRecord : equality>
        (logger: ILogger) as this =

            let _interiorSteps =
                new List<IStepHeader> ()

            member val InteriorSteps =
                _interiorSteps.AsReadOnly ()            


            // --- REQUIRED STEPS ---

            abstract member Opening :
                OpeningStep<'TPolicyRecord, 'TStepResults> with get

            abstract member OpeningRegression :
                SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with get

            abstract member RemoveExitedRecords :
                RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> with get 


            // --- FURTHER REQUIRED STEPS ---

            abstract member MoveToClosingExistingData :
                ClosingExistingDataStep<'TPolicyRecord, 'TStepResults> with get

            abstract member AddNewRecords :
                AddNewRecordsStep<'TPolicyRecord, 'TStepResults> with get


            // --- INTERIOR STEP REGISTRATION ---

            member private _._registerInteriorStep (step: IStepHeader) =

                do logger.LogDebug (sprintf "Registering interior step '%s'." step.Title)
                do _interiorSteps.Add step

            // DD - Using multiple dispatch we can control the types of interior steps we're expecting.
            member this.registerInteriorStep (step: SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
                do this._registerInteriorStep step
                step

            member this.registerInteriorStep (step: DataChangeStep<'TPolicyRecord, 'TStepResults>) =
                do this._registerInteriorStep step
                step


            member val AllSteps =
                // Must be a sequence or we get an error about using
                // members before they've been defined.
                seq {
                    yield this.Opening :> IStepHeader
                    yield this.OpeningRegression :> IStepHeader
                    yield this.RemoveExitedRecords :> IStepHeader
                    yield! this.InteriorSteps
                    yield this.MoveToClosingExistingData :> IStepHeader
                    yield this.AddNewRecords :> IStepHeader
                }
