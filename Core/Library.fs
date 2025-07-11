﻿
namespace AnalysisOfChangeEngine


/// Common types used throughout the analysis of change machinery.
[<AutoOpen>]
module Core =

    open System
    open System.Collections.Generic
    open System.Reflection
    open System.Threading.Tasks
    open FSharp.Quotations


    (*
    Design Decision:
        Why would we need these you ask?
        Given that the Guid type is used throughout, it's not impossible
        that (for example) an extraction UID suddently finds itself being
        used as a run UID. This should (!) reduce the chance of that happening.
    *)
    [<NoEquality; NoComparison>]
    type RunUid =
        | RunUid of Guid

        member this.Value =
            match this with
            | RunUid uid -> uid

    [<NoEquality; NoComparison>]
    type ExtractionUid =
        | ExtractionUid of Guid

        member this.Value =
            match this with
            | ExtractionUid uid -> uid

    [<NoEquality; NoComparison>]
    type StepUid =
        | StepUid of Guid

        member this.Value =
            match this with
            | StepUid uid -> uid


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type ApiRequestFailure =
        /// Indicates that, although a response was received from the API,
        /// it failed either in part or in entirety.
        | CalculationFailure of Reasons: string array
        /// Indicates that an API call failed to execute. This would,
        /// for example, be the case if the API was not available.
        | CallFailure of Reasons: string array

    type ApiRequestOutcome =
        Result<obj array, ApiRequestFailure>

    // Do NOT change this to IApiEndpoint. Again. The same endpoint could have
    // multiple requestors mapping to it. Calling it 'endpoint' would be misleading.
    type IApiRequestor<'TPolicyRecord> =
        interface
            /// Note that API requestors with the SAME NAME will be grouped together!
            abstract Name: string

            abstract ExecuteAsync:
                PropertyInfo array
                    -> 'TPolicyRecord
                    -> Task<ApiRequestOutcome>
        end


    // Allows us to define custom equality and comparison operations.
    // TODO - Is it risk only considering the Name for this operations?
    [<AbstractClass>]
    type AbstractApiRequestor<'TPolicyRecord> () =
        /// Note that API requestors with the SAME NAME will be grouped together!
        abstract member Name: string

        abstract member ExecuteAsync:
            PropertyInfo array
                -> 'TPolicyRecord
                -> Task<ApiRequestOutcome>
        
        override this.Equals other =
            match other with
            | :? AbstractApiRequestor<'TPolicyRecord> as other' ->
                (this :> IEquatable<_>).Equals other'
            | _ ->
                false

        override this.GetHashCode () =
            this.Name.GetHashCode ()

        interface IEquatable<AbstractApiRequestor<'TPolicyRecord>> with
            member this.Equals (other) =
                this.Name = other.Name

        interface IComparable<AbstractApiRequestor<'TPolicyRecord>> with
            member this.CompareTo other =
                this.Name.CompareTo other.Name

        interface IComparable with
            member this.CompareTo other =
                match other with
                | :? AbstractApiRequestor<'TPolicyRecord> as other' ->
                    (this :> IComparable<_>).CompareTo other'
                | _ ->
                    failwith "Cannot compare different types of API requestors."                    

        interface IApiRequestor<'TPolicyRecord> with
            member this.Name =
                this.Name

            member this.ExecuteAsync requiredOutputs policyRecord =
                this.ExecuteAsync requiredOutputs policyRecord


    [<RequireQualifiedAccess>]
    module AbstractApiRequestor =
        
        [<Sealed>]
        // No-one, and I mean no-one, should be deriving from this.
        type EncapsulatedApiRequestor<'TPolicyRecord> internal (name, asyncExecutor) =
            inherit AbstractApiRequestor<'TPolicyRecord> ()

            override _.Name =
                name

            override _.ExecuteAsync requiredOutputs policyRecord =
                asyncExecutor requiredOutputs policyRecord


        let create (name, asyncExecutor) : AbstractApiRequestor<'TPolicyRecord> =
            upcast new EncapsulatedApiRequestor<'TPolicyRecord> (name, asyncExecutor)


    // Allows us to extract the underlying requestor, even when we don't
    // have the step result type.
    type IWrappedApiRequestor<'TPolicyRecord> =
        interface
            abstract member UnderlyingRequestor:
                AbstractApiRequestor<'TPolicyRecord> with get
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
        | WrappedApiRequestor of AbstractApiRequestor<'TPolicyRecord>

        // Allows us to extract the underlying endpoint without caring about the response type.
        interface IWrappedApiRequestor<'TPolicyRecord> with

            member this.UnderlyingRequestor =
                match this with
                | WrappedApiRequestor abstractApiRequestor ->
                    abstractApiRequestor


    type ILogger =
        interface
            abstract member LogInfo: message: string -> unit
            abstract member LogDebug: message: string -> unit
            abstract member LogWarning: message: string -> unit
            abstract member LogError: message: string -> unit
        end


    [<AbstractClass>]
    // Private constructor as this will never be instantiated. It purely provides a container
    // for actions that can be performed as part of a source definition. Furthermore, having them
    // defined as instance members allows them to be more easily discoverable via reflection.
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


    (*
    Design Decision:
        Why not just use a Result type for this?
        Although a fair question... What would 'Ok' mean specifically? Using this specific DU,
        we make it clear that we care more about whether the validation completed or not.
    *)
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type StepValidationOutcome =
        /// Indicates that the validation logic was successfully applied, regardless of
        /// whether this led to validation errors being recognised (or not).
        | Completed of Errors: string array
        /// Indicates that the validation logic was unable to run for a specified reason.
        | Aborted of Reason: string

        /// Alias for a completed validation without any issues raised.
        static member val Empty =
            Completed [||] with get

    /// Step validator that receives the current policy record followed by
    /// the prior (where available) and current step results.
    type OpeningReRunStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults option * 'TStepResults -> StepValidationOutcome

    /// Step validator that receives the prior policy record and step results (where available),
    /// followed by those for the currene step.
    type DataChangeStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults option * 'TPolicyRecord * 'TStepResults -> StepValidationOutcome

    /// Step validator that receives the current policy record followed by
    /// the prior (where available) and current step results.
    type SourceChangeStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults option * 'TStepResults -> StepValidationOutcome

    /// Step validator that receives the new policy record and corresponding step results.
    type AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults -> StepValidationOutcome

    /// Helper function that provides no validation at all.
    let noValidator _ : StepValidationOutcome=
        StepValidationOutcome.Completed [||]


    /// Type definition for a data changer that takes the policy record as at the opening step,
    /// prior step and closing step. Return None if no record change is required. If a Some value
    /// is returned, it is always assumed that a data change has occurred.
    type PolicyRecordChanger<'TPolicyRecord> =
        // Opening * Prior * Closing -> Optional Revised
        'TPolicyRecord * 'TPolicyRecord * 'TPolicyRecord -> Result<'TPolicyRecord option, string>


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


    /// Required first step (usually considered step #0).
    [<NoEquality; NoComparison>]
    type OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        {
            Uid             : Guid
            Title           : string
            Description     : string
            Source          : OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator       : OpeningReRunStepValidator<'TPolicyRecord, 'TStepResults>
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


    (*
    Design decision:
        Arguably, do we need to have a separate step for this? One alternative would be for
        us to just "know" that exited policies are removed after the opening re-run step.
        However, having a dedicated step for this makes it explicit.
        We have a similar situation later with the add new records step.
    *)
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
                        Ok None
                    else
                        Ok (Some closing)


    (*
    Design Decision:
        As with the remove exited records step, do we need a seperate step for this?
        Could we not just have them "appear" as part of the move to closing data step?
        Firstly, new records would have a different validation function signature compared
        to existing records. Again, having a dedicated step makes it explicit; the user
        doesn't need to remember that new records magically appear as part of a different step.
    *)
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


    type IWalk =
        interface
            abstract member AllSteps : IStepHeader seq
        end


    /// Abstract base class for all user defined walks. Provides 'slots' for required steps
    /// and a mechanism by which additional (ie. user supplied) steps can be registered.
    [<AbstractClass>]
    type AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord : equality>
        (logger: ILogger) as this =

            let _interiorSteps =
                new List<IStepHeader> ()       


            /// Required step.
            abstract member OpeningReRun :
                OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with get

            /// Required step.
            abstract member RemoveExitedRecords :
                RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> with get

            // User defined interior steps will be defined within the
            // derived class and tracked via the 'registerInteriorStep'
            // function below.
            
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
                    yield this.OpeningReRun :> IStepHeader
                    yield this.RemoveExitedRecords :> IStepHeader
                    yield! _interiorSteps
                    yield this.MoveToClosingData :> IStepHeader
                    yield this.AddNewRecords :> IStepHeader
                } with get

            interface IWalk with
                member this.AllSteps =
                    this.AllSteps


    [<NoEquality; NoComparison>]
    type ExitedPolicyId =
        | ExitedPolicyId of string
        
        member this.Value =
            match this with
            | ExitedPolicyId id -> id

    [<NoEquality; NoComparison>]
    type RemainingPolicyId =
        | RemainingPolicyId of string
        
        member this.Value =
            match this with
            | RemainingPolicyId id -> id

    [<NoEquality; NoComparison>]
    type NewPolicyId =
        | NewPolicyId of string
        
        member this.Value =
            match this with
            | NewPolicyId id -> id


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type PolicyGetterFailure =
        | ParseFailure of Reason: string
        | NotFound

    type PolicyGetterOutcome<'TPolicyRecord> =
        Result<'TPolicyRecord, PolicyGetterFailure>

    // We could just use a simple function signature here. However,
    // if this needs to be disposable, easier if it's an interface.
    type IPolicyGetter<'TPolicyRecord> =
        interface
            abstract member GetPolicyRecordsAsync :
                policyIds : string array
                    -> Task<Map<string, PolicyGetterOutcome<'TPolicyRecord>>>
        end


    [<NoEquality; NoComparison>]
    type ExitedPolicy<'TPolicyRecord> =
        | ExitedPolicy of Opening: 'TPolicyRecord

        member this.PolicyRecord =
            match this with
            | ExitedPolicy policyRecord ->
                policyRecord

    [<NoEquality; NoComparison>]
    type RemainingPolicy<'TPolicyRecord> =
        | RemainingPolicy of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord

        member this.PolicyRecords =
            match this with
            | RemainingPolicy (openingPolicyRecord, closingPolicyRecord) ->
                openingPolicyRecord, closingPolicyRecord                
        
    [<NoEquality; NoComparison>]
    type NewPolicy<'TPolicyRecord> =
        | NewPolicy of Closing: 'TPolicyRecord

        member this.PolicyRecord =
            match this with
            | NewPolicy policyRecord ->
                policyRecord


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type EvaluationFailure =
        | ApiCalculationFailure     of RequestorName: string    * Reasons: string array
        | ApiCallFailure            of RequestorName: string    * Reasons: string array
        | DataChangeFailure         of StepHeader: IStepHeader  * Reasons: string array
        | ValidationAborted         of StepHeader: IStepHeader  * Reasons: string array
        | ValidationFailure         of StepHeader: IStepHeader  * Reasons: string array
        | StepConstructionFailure   of StepHeader: IStepHeader  * Reasons: string array
        | Cancelled


    // We use lists for the failures as they can be more easily constructed
    // in a cumulative manner.
    type ExitedPolicyOutcome<'TStepResults> =
        Result<'TStepResults, EvaluationFailure list>

    type RemainingPolicyOutcome<'TStepResults> =
        Result<'TStepResults array, EvaluationFailure list>

    type NewPolicyOutcome<'TStepResults> =
        Result<'TStepResults, EvaluationFailure list>


    type IPolicyEvaluator<'TPolicyRecord, 'TStepResults> =
        interface
            /// Only a single result will be provided for the initial
            /// (ie. opening re-run) step.
            abstract member execute:
                ExitedPolicy<'TPolicyRecord>
                    -> Task<ExitedPolicyOutcome<'TStepResults>>

            abstract member execute:
                RemainingPolicy<'TPolicyRecord>
                    -> Task<RemainingPolicyOutcome<'TStepResults>>

            /// Only a single result will be provided for the final
            /// (ie. add new records) step.
            abstract member execute:
                NewPolicy<'TPolicyRecord>
                    -> Task<NewPolicyOutcome<'TStepResults>>
        end
