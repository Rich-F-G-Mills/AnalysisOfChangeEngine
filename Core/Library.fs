
namespace AnalysisOfChangeEngine




/// Common types used throughout the analysis of change machinery.
// This codifies the underlying philosophy of what we're trying to achieve here!
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
        The YouTuber 'Coding Jesus' would be proud of this approach, which he
        refers to as 'strong typing'.
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
        /// Cancelled by the user.
        | Cancelled

    /// Represents the outcome of an asynchronous execution
    /// performed by an IApiRequestor object,
    type ApiRequestOutcome =
        Result<obj array, ApiRequestFailure>


    [<NoEquality; NoComparison>]
    /// Provides timing information for a given API request. This will always be provided,
    /// irrespective of whether the request was successful or not.
    type ApiRequestTelemetry =
        {
            /// A given API requestor may have multiple end-points that can be used.
            /// This allows the one actually used to be specified.
            EndpointId      : string option
            ProcessingStart : DateTime
            ProcessingEnd   : DateTime
        }


    (*
    Design Decision:
        Why not use an interface instead?
        We want to use the instances for grouping operations. By using an
        abstract class, we can bake-in implementations for eqality and
        comparison operations.

        Is it risk only considering the Name for this operations?
        TODO - Could reference equality be used for grouping oeprations?

        Do NOT change this to IApiEndpoint. Again. The same endpoint could have
        multiple requestors mapping to it. Calling it 'endpoint' would be misleading.
    *)

    /// Types implementing this interface provide an asynchronous way to submit a
    /// calculation request to an API end-point.
    [<AbstractClass>]
    type AbstractApiRequestor<'TPolicyRecord> () =
        /// Note that API requestors with the SAME NAME will be grouped together!
        abstract member Name: string

        /// Asyncronously submit a calculation request to the underlying end-point.
        abstract member ExecuteAsync:
            PropertyInfo array
                -> 'TPolicyRecord
                // We don't insist that telemetry is provided. There are situations where
                // no request was even made/possible. This can be indicated as such here.
                -> Task<ApiRequestOutcome * ApiRequestTelemetry option>
        
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


    [<RequireQualifiedAccess>]
    /// Contains logic providing for the creation of API requestors.
    module ApiRequestor =
        
        [<Sealed>]
        // No-one, and I mean no-one, should be deriving from this.
        /// Not intended for direct developer use.
        type EncapsulatedApiRequestor<'TPolicyRecord> internal (name, asyncExecutor) =
            inherit AbstractApiRequestor<'TPolicyRecord> ()

            override _.Name =
                name

            override _.ExecuteAsync requiredOutputs policyRecord =
                asyncExecutor requiredOutputs policyRecord

        /// Create an API requestor using the supplied name and executor logic.
        let create (name, asyncExecutor) : AbstractApiRequestor<'TPolicyRecord> =
            upcast new EncapsulatedApiRequestor<'TPolicyRecord> (name, asyncExecutor)


    // This effectively allows us to peel away the generic API
    // response type from a wrapped API requestor.
    type IWrappedApiRequestor<'TPolicyRecord> =
        interface
            abstract member UnderlyingRequestor:
                AbstractApiRequestor<'TPolicyRecord> with get
        end


    (*
    Design Decision:
        We also need to track the possible API responses... We cannot use
        a vanilla type alias as it will complain about TResponse not actually getting used.
        However, we have no such issue using a DU. Ultimately, this is just an API requestor
        with some supplementary type information for intelli-sense purposes.
    *)
    /// Represents an API requestor which, via a "baked-in" generic type-parameter, provides
    /// a strongly typed link to possible responses.
    [<NoEquality; NoComparison>]
    type WrappedApiRequestor<'TPolicyRecord, 'TResponse> =
        | WrappedApiRequestor of AbstractApiRequestor<'TPolicyRecord>

        // As mentioned above, allows us to peel away the generic response type
        // which we don't _actually_ care about.
        interface IWrappedApiRequestor<'TPolicyRecord> with

            member this.UnderlyingRequestor =
                match this with
                | WrappedApiRequestor abstractApiRequestor ->
                    abstractApiRequestor


    type ILogger =
        interface
            abstract member LogInfo     : message: string -> unit
            abstract member LogDebug    : message: string -> unit
            abstract member LogWarning  : message: string -> unit
            abstract member LogError    : message: string -> unit
        end


    [<AbstractClass>]
    // Private constructor as this will NEVER be instantiated. It purely provides a container
    // for actions that can be performed as part of a source definition. Furthermore, having them
    // defined as instance members allows them to be more easily discoverable via reflection.
    type SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection> private () =
        (*
        Design Decision:
            Could have used a curried call rather than tupled; however, identifying (let along deciphering)
            it within any code quotation where it was used was proving a lot more complicated.

            Furthermore... Why would we not just directly pass in the wrapped API requestor?
            Firstly, we are limited what can be passed in to a code quotation. When we
            compile the walk, we pass in a collection of API requestors. We can therefore
            provide a mapping from this collection to one of the requestor within. Put another
            way, it is trivial to quote the accessing of an object's property.
        *)
        /// Request specific output from the referenced API end-point and corresponding response.        
        abstract member apiCall<'TResponse, 'T>
            : apiRequest: ('TApiCollection -> WrappedApiRequestor<'TPolicyRecord, 'TResponse>) * selector: ('TResponse -> 'T) -> 'T


    /// Required type of all source definitions for the opening re-run step.
    type OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> 'TPolicyRecord   // Current policy record.
            -> 'TStepResults    // Current results.
            -> 'TStepResults>   // Constructed results for current step.

    [<RequireQualifiedAccess>]
    module private OpeningReRunSourceExpr =
        // Used to break apart a source definition.
        let internal (|Definition|_|) (sourceExpr: OpeningReRunSourceExpr<_, 'TStepResults, _>) =
            match sourceExpr with
            | Patterns.Lambda (from,
                Patterns.Lambda (policyRecord,
                        Patterns.Lambda (currentResults, sourceBody))) ->
                            let sourceBody' =
                                Expr.Cast<'TStepResults> sourceBody

                            Some (from, policyRecord, currentResults, sourceBody')
            | _ ->
                None


    /// Required type of all source definitions (except for the opening re-run step).
    type SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> 'TPolicyRecord   // Current policy record.
            -> 'TStepResults    // Prior source definition.
            -> 'TStepResults    // Current source definition.
            -> 'TStepResults>   // Constructed results for current step.

    [<RequireQualifiedAccess>]
    module SourceExpr =
        /// Not intended for direct developer use.
        let cast<'TPolicyRecord, 'TStepResults, 'TApiCollection> expr
            : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                Expr.Cast<_> expr

        // Used to break apart a source definition.
        /// Not intended for direct developer use.
        let (|Definition|_|) (sourceExpr: SourceExpr<_, 'TStepResults, _>) =
            match sourceExpr with
            | Patterns.Lambda (from,
                Patterns.Lambda (policyRecord,
                    Patterns.Lambda (priorResults,
                        Patterns.Lambda (currentResults, sourceBody)))) ->
                            let sourceBody' =
                                Expr.Cast<'TStepResults> sourceBody

                            Some (from, policyRecord, priorResults, currentResults, sourceBody')
            | _ ->
                None


    (*
    Design Decision:
        Why not just use a Result type for this?
        Although a fair question... What would 'Ok' mean specifically? Using this specific DU,
        we make it clear that we care more about whether the validation completed or not.
    *)
    /// Represents the outcome of step validation logic.
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

    /// Step validator that receives the prior policy record and step results,
    /// followed by those for the current step. Note that this will ONLY be run
    /// if a data change has actually occurred at this step.
    type DataChangeStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults * 'TPolicyRecord * 'TStepResults -> StepValidationOutcome

    /// Step validator that receives the current policy record followed by
    /// the prior  and current step results.
    type SourceChangeStepValidator<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationOutcome

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


    /// Required first step. In the absence of (for example) model changes or
    /// errors in the underlying logic, this would be expected to tie up
    /// the prior run's closing position.
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


    /// Underlying type of the penultimate (and required) step in the walk.
    /// Indicates a move to the closing data for a given policy record.
    [<NoEquality; NoComparison>]
    // We need the equality constraint here as we need to be able to compare closing policy
    // records against those inherited from the prior step.
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
                        // If there's no change in the policy record, then indicate as such.
                        Ok None
                    else
                        // Conversely, return the modified record.
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


    /// Automatically implemented by all walks. Allows for the extraction of
    /// all registered step headers without needing to know (or care) about
    /// the various generic type parameters as used by an abstract walk instance.
    type IWalk =
        interface
            abstract member AllSteps : IStepHeader seq
        end


    /// Abstract base class for all user defined walks. Provides 'slots' for required steps
    /// and a mechanism by which additional (ie. user supplied) steps can (and MUST!)
    /// be registered.
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
                // Not clear how this logging is adding any value. Suppressing for now.
                // do logger.LogDebug (sprintf "Registering interior step '%s'." step.Title)
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
        /// Indicates that, although the required record was found, it could
        /// not be successfully parsed into a corresponding policy record object.
        | ParseFailure of Reasons: string array
        /// No record could be found with the requisite ID.
        | NotFound
        /// User cancelled the request.
        | Cancelled

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
        /// Indicates that, although a response was received from the API,
        /// it failed either in part or in entirety.
        | ApiCalculationFailure     of RequestorName: string    * Reasons: string array        
        /// Indicates that an API call failed to execute. This would,
        /// for example, be the case if the API was not available.
        | ApiCallFailure            of RequestorName: string    * Reasons: string array
        /// A data-change step could not successfully transform the policy record
        /// for a given step.
        | DataChangeFailure         of StepHeader: IStepHeader  * Reasons: string array
        /// Indicates that the validation logic was successfully applied with errors
        /// having been identified.
        | ValidationFailure         of StepHeader: IStepHeader  * Reasons: string array
        /// Indicates that the validation logic was unable to run for a specified reason.
        | ValidationAborted         of StepHeader: IStepHeader  * Reasons: string array
        /// It was not possible to construct a step result for a given policy.
        | StepConstructionFailure   of StepHeader: IStepHeader  * Reasons: string array
        /// User cancelled the request.
        | Cancelled


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type StepDataSource =
        | OpeningData
        | ClosingData
        // Theoretically, we should use the data changer interface. However,
        // given it's generic with respect to the policy record type, we'd
        // then need to make this generic as well.
        | DataChangeStep of IStepHeader
        

    type EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults> =
        {
            // Strictly speaking, for the interior data changes, we should use DataChangeSteps rather than UIDs within the map.
            // However, for the sake of acceptable pragmatism, the UID will work fine for our purposes.
            InteriorDataChanges : Map<Guid, 'TPolicyRecord>
            StepResults         : Map<Guid, StepDataSource * 'TStepResults>
        }

    // Either all steps succeed, or we return a list of failures.
    type PolicyWalkOutcome<'TPolicyRecord, 'TStepResults> =
        // This makes it explicit that we're only return internal data stages.
        Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, EvaluationFailure list>


    [<NoEquality; NoComparison>]
    type EvaluationApiRequestTelemetry =
        {
            RequestorName   : string
            DataSource      : StepDataSource
            EndpointId      : string option
            Submitted       : DateTime
            ProcessingStart : DateTime
            ProcessingEnd   : DateTime
        }       

    (*
    Design Decision:
        Why not just make the policy evaluator an observable?
        We could... However, I want it to be possible for the telemetry to
        be associated with a given polcy ID. This would mean passing in policy ID
        to logic that strictly couldn't care less about the policy ID being run.
        Regardless of whether we use the current approach or the observable approach,
        we're still going to be making allocations for telemetry output.
    *)
    [<NoEquality; NoComparison>]
    type EvaluationTelemetry =
        {       
            EvaluationStart     : DateTime
            EvaluationEnd       : DateTime
            ApiRequestTelemetry : EvaluationApiRequestTelemetry list
        }

    
    /// Required interface for any implementation that can
    /// asyncronously evaluate a given policy.
    // We can't replace this with a function signature as we require different inputs depending
    // on the cohort to which the given policy belongs.
    type IPolicyEvaluator<'TPolicyRecord, 'TStepResults> =
        interface
            /// Only a single result will be provided for the initial
            /// (ie. opening re-run) step.
            abstract member Execute:
                ExitedPolicy<'TPolicyRecord> * 'TStepResults option
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults> * EvaluationTelemetry>

            abstract member Execute:
                // We can optionally provide the prior closing step results.
                RemainingPolicy<'TPolicyRecord> * 'TStepResults option
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults> * EvaluationTelemetry>

            /// Only a single result will be provided for the final
            /// (ie. add new records) step.
            abstract member Execute:
                NewPolicy<'TPolicyRecord>
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults> * EvaluationTelemetry>
        end