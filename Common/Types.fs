
namespace AnalysisOfChangeEngine


/// Common types used throughout the analysis of change machinery.
[<AutoOpen>]
module Types =

    open System
    open System.Collections.Generic
    open System.Reflection
    open FSharp.Quotations


    type PolicyID = string
    type Reason = string


    /// Interface to be implemented for all user defined policy record types.
    type IPolicyRecord =
        interface
            abstract member ID: PolicyID with get
        end


    /// Details of the context within which the walk is to be run.
    [<NoEquality; NoComparison>]
    type RunContext =
        {
            OpeningRunDate: DateOnly
            ClosingRunDate: DateOnly
        }
      

    // Do NOT change this to IApiEndpoint. Again. The same endpoint could have
    // multiple requestors mapping to it. Calling it 'endpoint' would be misleading.
    type IApiRequestor<'TPolicyRecord> =
        interface
            /// Note that API requestors with the SAME NAME will be grouped together!
            abstract Name: string
            abstract Execute: Map<string, PropertyInfo> -> 'TPolicyRecord -> Result<Map<string, obj>, string>
        end

    (*
    Design Decision:
        We also need to track the possible API responses... We cannot use
        a vanilla type alias as it will complain about TResponse not actually getting used.
        However, we have no such issue using a DU. Ultimately, this is just an IApiRequestor
        with some supplementary type information for intelli-sense purposes.
    *)
    /// Represent an API end-point which, via a "baked-in" generic type-parameter, provides
    /// a strongly typed link to possible responses.
    [<NoEquality; NoComparison>]
    type WrappedApiRequestor<'TPolicyRecord, 'TResponse> =
        | WrappedApiRequestor of IApiRequestor<'TPolicyRecord>

        // Allows us to extract the underlying endpoint without caring about the response type.
        interface IApiRequestor<'TPolicyRecord> with
            member this.Name =
                match this with
                | WrappedApiRequestor inner ->
                    inner.Name

            member this.Execute pis policyRecord =
                match this with
                | WrappedApiRequestor inner ->
                    inner.Execute pis policyRecord
            

    type ILogger =
        interface
            abstract member LogInfo: message: string -> unit
            abstract member LogDebug: message: string -> unit
            abstract member LogWarning: message: string -> unit
            abstract member LogError: message: string -> unit
        end


    /// Possible issues identified when applying step validation logic.
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type StepValidationIssue =
        | Warning of string
        | Error of string


    [<AbstractClass>]
    type SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection> private () =
        (*
        Design Decision:
            Could have used a curried call rather than tupled; however, identifying (let along deciphering)
            it within any code quotation where it was used was proving a lot more complicated.
        *)
        /// Request specific output from the referenced API end-point and corresponding response.        
        abstract member apiCall<'TResponse, 'T>
            : apiRequest: ('TApiCollection -> WrappedApiRequestor<'TPolicyRecord, 'TResponse>) * selector: ('TResponse -> 'T) -> 'T


    /// Required type of all source definitions.
    type OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> 'TPolicyRecord   // Current policy record.
            -> 'TStepResults    // Current results.
            -> 'TStepResults>   // Constructed results for current step.

    [<RequireQualifiedAccess>]
    module OpeningReRunSourceExpr =
        // Used to break apart a source definition.
        let (|Definition|_|) = function
            | Patterns.Lambda (from,
                Patterns.Lambda (policyRecord,
                        Patterns.Lambda (currentResults, sourceBody))) ->
                            Some (from, policyRecord, currentResults, sourceBody)
            | _ ->
                None


    /// Required type of all source definitions.
    type SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> 'TPolicyRecord   // Current policy record.
            -> 'TStepResults    // Prior source definition.
            -> 'TStepResults    // Current source definition.
            -> 'TStepResults>   // Constructed results for current step.

    [<RequireQualifiedAccess>]
    module SourceExpr =
        // Used to break apart a source definition.
        let (|Definition|_|) = function
            | Patterns.Lambda (from,
                Patterns.Lambda (policyRecord,
                    Patterns.Lambda (priorResults,
                        Patterns.Lambda (currentResults, sourceBody)))) ->
                            Some (from, policyRecord, priorResults, currentResults, sourceBody)
            | _ ->
                None

        let cast<'TPolicyRecord, 'TStepResults, 'TApiCollection> expr
            : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                Expr.Cast<_> expr


    /// Step validator that receives the opening policy record and corresponding step results.
    type OpeningStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    /// Step validator that receives the prior policy record and step results,
    /// followed by those for the currene step.
    type DataChangeStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults * 'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    /// Step validator that receives the current policy record followed by
    /// the prior and current step results.
    type SourceChangeStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationIssue list

    /// Step validator that receives the new policy record and corresponding step results.
    type AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list


    /// Helper function that provides no validation at all.
    let noValidator _ : StepValidationIssue list =
        List.empty


    /// Type definition for a data changer that takes the policy record as at the opening step,
    /// prior step and closing step. Return None if no record change is required. If a Some value
    /// is returned, it is always assumed that a data change has occurred.
    type PolicyRecordChanger<'TPolicyRecord> =
        // Opening * Prior * Closing -> Optional Revised
        'TPolicyRecord * 'TPolicyRecord * 'TPolicyRecord -> 'TPolicyRecord option


    /// Required interface for all step types.
    type IStepHeader =
        interface
            abstract member Uid         : Guid with get
            abstract member Title       : string with get
            abstract member Description : string with get
        end


    /// Required interface for any step that has a source definition.
    type ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        interface
            inherit IStepHeader
            abstract member Source: SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        end


    /// Required interface for steps that can modify policy records. This does NOT
    /// relate to steps which change whether a policy record is included (or not).
    type IDataChangeStep<'TPolicyRecord> =
        interface
            inherit IStepHeader
            abstract member DataChanger: PolicyRecordChanger<'TPolicyRecord> with get
        end


    (*
    Design Decision:
        Arguably, this could be considered to be a data change step. However, given
        it's the opening step, we can't change data that didn't actually exist
        prior to this point.
    *)
    /// Required first step (usually considered step #0).
    [<NoEquality; NoComparison>]
    type OpeningStep<'TPolicyRecord, 'TStepResults> =
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


    /// Required first step (usually considered step #0).
    [<NoEquality; NoComparison>]
    type OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            Source          : OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator       : SourceChangeStepValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description

        interface ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with
            member this.Source =
                match this.Source with
                | OpeningReRunSourceExpr.Definition
                    (from, policyRecord, currentResults, sourceBody) ->
                        let untypedExpr =
                            Expr.Lambda (from,
                                Expr.Lambda (policyRecord,
                                    // This is solely to make sure our lambda has the correct signature.
                                    Expr.Lambda (Var ("priorResults", typeof<'TStepResults>),
                                        Expr.Lambda (currentResults, sourceBody))))

                        SourceExpr.cast<_, _, _> untypedExpr

                | _ ->
                    failwith "Invalid source defintion."


    /// Indicates a step where the source can be specified.
    [<NoEquality; NoComparison>]
    type SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
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

        interface ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with
            member this.Source =
                this.Source
                

    /// Indicates a step where the current policy record can be changed in a specified way.
    /// This cannot change whether a policy is included (or not) for processing.
    [<NoEquality; NoComparison>]
    type DataChangeStep<'TPolicyRecord, 'TStepResults> =
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


    /// Indicates that policies not present in the closing position are to be excluded
    /// from further processing.
    [<NoEquality; NoComparison>]
    type RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
        }

        interface IStepHeader with
            member this.Uid = this.Uid
            member this.Title = this.Title
            member this.Description = this.Description 


    /// Underlying type of the penultimate (and required) step in the walk.
    /// Indicates a move to the closing data for a given policy record.
    [<NoEquality; NoComparison>]
    type MoveToClosingDataStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality> =
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
                fun (_, prior, closing) ->
                    if prior = closing then
                        None
                    else
                        Some closing


    /// Underlying type of the final (and required) step where policies only
    /// present in the closing position are valued.
    [<NoEquality; NoComparison>]
    type AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
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


    /// Abstract base class for all user defined walks. Provides 'slots' for required steps
    /// and a mechanism by which additional (ie. user supplied) steps can be registered.
    [<AbstractClass>]
    type AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord and 'TPolicyRecord : equality>
        (logger: ILogger) as this =

            let _interiorSteps =
                new List<IStepHeader> ()

            member val InteriorSteps =
                _interiorSteps.AsReadOnly ()            


            /// Required step.            
            abstract member Opening :
                OpeningStep<'TPolicyRecord, 'TStepResults> with get

            /// Required step.
            abstract member OpeningReRun :
                OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with get

            /// Required step.
            abstract member RemoveExitedRecords :
                RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> with get 

            
            /// Required step.
            abstract member MoveToClosingData :
                MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> with get

            /// Required step.
            abstract member AddNewRecords :
                AddNewRecordsStep<'TPolicyRecord, 'TStepResults> with get


            member private _._registerInteriorStep (step: IStepHeader) =

                do logger.LogDebug (sprintf "Registering interior step '%s'." step.Title)
                do _interiorSteps.Add step

            (*
            Design Decision:
                Using multiple dispatch, we can control the permitted types used for interior
                (ie. user defined) steps.
            *)
            /// Register an interior source change step.
            member this.registerInteriorStep (step: SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
                do this._registerInteriorStep step
                step

            /// Register an interior data change change step.
            member this.registerInteriorStep (step: DataChangeStep<'TPolicyRecord, 'TStepResults>) =
                do this._registerInteriorStep step
                step


            /// Provides an enumeration of all steps, both required and user supplied in the appropriate order.
            member val AllSteps =
                // Must be a sequence or we get an error about using
                // members before they've been defined.
                seq {
                    yield this.Opening :> IStepHeader
                    yield this.OpeningReRun :> IStepHeader
                    yield this.RemoveExitedRecords :> IStepHeader
                    yield! this.InteriorSteps
                    yield this.MoveToClosingData :> IStepHeader
                    yield this.AddNewRecords :> IStepHeader
                }
