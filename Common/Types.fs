
namespace AnalysisOfChangeEngine.Common


[<AutoOpen>]
module rec Types =

    open System
    open System.Collections.Generic
    open System.Reflection
    open FSharp.Quotations


    type PolicyID = string
    type Reason = string
    type CleansingChanges = CleansingChange list

    type ValidatedPolicyRecord<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        | ValidatedPolicyRecord of 'TPolicyRecord

    type ParseOutcome<'T> =
        Result<'T * CleansingChanges, PolicyID option * Reason>

    [<NoEquality; NoComparison>]
    type RunContext =
        {
            OpeningRunDate: DateOnly
            ClosingRunDate: DateOnly
        }


    // Whereas we want to log cleansing changes, we're not interested in loggin
    // data changes made as part of the data roll-forward between the opening
    // and closing position.
    [<NoEquality; NoComparison>]
    type CleansingChange =
        {
            Field:  string
            From:   string
            To:     string
            Reason: string
        }

    type IPolicyRecord =
        interface
            abstract member ID: string with get
        end


    type ApiRequest<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        Map<string, PropertyInfo> -> 'TPolicyRecord -> Result<Map<string, obj>, string>

    type IUnwrappableApiRequest<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        interface
            abstract Name: string
            abstract Requestor: ApiRequest<'TPolicyRecord>
        end

    // DD - Arguably this could be an interface. However, we're not intending for
    // anything to inherit this, so a record suits our purposes fine. Furthermore,
    // we need to pass in the response type in order for intellisense to work
    // when users are creating the source specifications.
    [<NoEquality; NoComparison>]
    type WrappedApiRequest<'TPolicyRecord, 'TResponse when 'TPolicyRecord :> IPolicyRecord> =
        | WrappedApiRequest of string * ApiRequest<'TPolicyRecord>

        interface IUnwrappableApiRequest<'TPolicyRecord> with
            member this.Name =
                match this with
                | WrappedApiRequest (name, _) -> name

            member this.Requestor =
                match this with
                | WrappedApiRequest (_, requestor) -> requestor
            

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
    type SourceAction<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> private () =
        /// Request specific output from the referenced API.
        abstract member apiCall<'TResponse, 'T>
            : apiRequest: WrappedApiRequest<'TPolicyRecord, 'TResponse> * selector: ('TResponse -> 'T) -> 'T

        /// Permits fields to be calculated using other fields within the step output.
        abstract member calculation<'T>
            : definition: ('TStepResults -> 'T) -> 'T


    type SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        // DD - If we supply 'from' and 'apis' as tupled arguments, the resulting quotation
        // is more cumbersome to process. Using curried form makes them easier to identify.
        Expr<SourceAction<'TPolicyRecord, 'TStepResults> -> 'TApiCollection -> 'TStepResults -> 'TStepResults>

    module SourceDefinition =
        let castExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> expr
            : SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                Expr.Cast<_> expr


    // The only items available for validation will be the record itself and the corresponding results.
    type OpeningStepValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    // We have both before and after data along with corresponding step results.
    type DataChangeValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    // For a regression step, we aren't changing policy data. As such, we're only
    // expecting variability due to a change in the underlying API.
    type RegressionValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationIssue list

    // Similar situation as for the regression validator above.
    type ParameterChangeValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationIssue list

    // Similar situation to the open step validator above.
    type AddNewRecordsValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
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
        'TPolicyRecord * 'TPolicyRecord * 'TPolicyRecord -> 'TPolicyRecord option


    type IStepHeader =
        interface
            abstract member Title: string with get
            abstract member Description: string with get
        end


    // The opening data will be specified outside of this. We need only worry
    // about validation.
    [<NoEquality; NoComparison>]
    type OpeningStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Validator: OpeningStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description                

    // We'd usually run a regression test because of a change in parameterisation
    // of the underlying API. As such, no data changes are expected (nor permitted)
    // for these.
    [<NoEquality; NoComparison>]
    type RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator: RegressionValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description    

    // Intended this would be used as part of an opening data restatement along
    // will subsequent data changes in order to walk to the closing data.
    [<NoEquality; NoComparison>]
    type DataChangeStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            DataChanger: PolicyRecordChanger<'TPolicyRecord>
            Validator: DataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    // Intended that a parameter change is achieved via a change in the underlying API.
    [<NoEquality; NoComparison>]
    type ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator: ParameterChangeValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    // No data changer or validator needed here.
    [<NoEquality; NoComparison>]
    type RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type InteriorStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        | Regression of Step: RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        | DataChange of Step: DataChangeStep<'TPolicyRecord, 'TStepResults>
        | ParameterChange of Step: ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>     

        interface IStepHeader with
            member this.Title =
                match this with
                | Regression { Title = title }
                | DataChange { Title = title }
                | ParameterChange { Title = title } ->
                    title

            member this.Description =
                match this with
                | Regression { Description = description }
                | DataChange { Description = description }
                | ParameterChange { Description = description } ->
                    description

    // This a catch-all to ensure that there are no outstanding changes in order
    // to reach the closing data. Given we're only changing the underlying policy
    // data, no change in underlying API expected and validator signature matches
    // this reasoning.
    [<NoEquality; NoComparison>]
    type MoveToClosingExistingDataStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Validator: DataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description

    // Again, no change in data or API allowed (or permitted). All we're doing is
    // changing the filter applied as to whether a given record is considered for
    // processing.
    [<NoEquality; NoComparison>]
    type AddNewRecordsStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Validator: AddNewRecordsValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description


    [<AbstractClass>]
    type AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection  when 'TPolicyRecord :> IPolicyRecord>
        (logger: ILogger) =

            let _interiorSteps =
                new List<InteriorStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> ()


            // --- REQUIRED STEPS ---

            abstract member opening :
                OpeningStep<'TPolicyRecord, 'TStepResults> with get

            abstract member openingRegression :
                RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with get

            abstract member removeExitedRecords :
                RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> with get 


            // --- FURTHER REQUIRED STEPS ---

            abstract member moveToClosingExistingData :
                MoveToClosingExistingDataStep<'TPolicyRecord, 'TStepResults> with get

            abstract member addNewRecords :
                AddNewRecordsStep<'TPolicyRecord, 'TStepResults> with get


            // --- INTERIOR STEP REGISTRATION ---

            member private _.registerInteriorStep (step: InteriorStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
                let stepHeader =
                    step :> IStepHeader

                do logger.LogDebug (sprintf "Registering interior step '%s'." stepHeader.Title)
                do _interiorSteps.Add step

            // Via multiple dispatch we can control the types of interior steps
            // we're expecting.
            member this.registerInteriorStep (step: RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
                do this.registerInteriorStep (InteriorStep.Regression step)
                step

            member this.registerInteriorStep (step: DataChangeStep<'TPolicyRecord, 'TStepResults>) =
                do this.registerInteriorStep (InteriorStep.DataChange step)
                step

            member this.registerInteriorStep (step: ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
                do this.registerInteriorStep (InteriorStep.ParameterChange step)
                step
