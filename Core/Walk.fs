
namespace AnalysisOfChangeEngine

open System
open System.Collections.Generic
open System.Threading.Tasks


/// Automatically implemented by all walks. Allows for the extraction of
/// all registered step headers without needing to know (or care) about
/// the various generic type parameters as used by an abstract walk instance.
type IWalk =
    interface
        abstract member AllSteps    : IStepHeader seq
        abstract member ClosingStep : IStepHeader with get
    end


/// Abstract base class for all user defined walks. Provides 'slots' for required steps
/// and a mechanism by which additional (ie. user supplied) steps can (and MUST!)
/// be registered.
[<AbstractClass>]
type AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord : equality>
    (logger: ILogger) as this =

        let _interiorSteps =
            new List<IStepHeader> ()   
            
        // We only permit source change steps to be used after the closing step.
        let _postNewRecordsSteps =
            new List<SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> ()


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
            Using function overloads, we can control the permitted types used for interior
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


        // We only permit source change steps at this point.
        member this.registerPostNewRecordsStep (step: SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
            do _postNewRecordsSteps.Add step
            step


        /// Provides an enumeration of all steps, both required and user
        /// supplied in the appropriate order.
        member val AllSteps =
            // Must be a sequence or we get an error about using
            // members before they've been defined.
            seq {
                yield this.OpeningReRun :> IStepHeader
                yield this.RemoveExitedRecords :> IStepHeader
                yield! _interiorSteps
                yield this.MoveToClosingData :> IStepHeader
                yield this.AddNewRecords :> IStepHeader
                yield! _postNewRecordsSteps |> Seq.cast<IStepHeader>
            } with get

        /// This is the step header for what is considered to be the closing
        /// step for the walk, and is therefore considered for comparison
        /// against the subsequent opening re-run step for the following period.
        /// However, this does not prevent additional steps being run beyond this.
        abstract member ClosingStep: IStepHeader with get

        interface IWalk with
            member this.AllSteps =
                this.AllSteps

            member this.ClosingStep =
                this.ClosingStep


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type WalkEvaluationFailure =
    /// Indicates that, although a response was received from the API,
    /// it failed either in part or in entirety.
    | ApiCalculationFailure     of RequestorName: string    * Reasons: string list        
    /// Indicates that an API call failed to execute. This would,
    /// for example, be the case if the API was not available.
    | ApiCallFailure            of RequestorName: string    * Reasons: string list
    /// A data-change step could not successfully transform the policy record
    /// for a given step.
    | DataChangeFailure         of StepHeader: IStepHeader  * Reasons: string list
    /// Indicates that the validation logic was successfully applied with errors
    /// having been identified.
    | ValidationFailure         of StepHeader: IStepHeader  * Reasons: string list
    /// Indicates that the validation logic was unable to run for a specified reason.
    | ValidationAborted         of StepHeader: IStepHeader  * Reason: string
    /// It was not possible to construct a step result for a given policy.
    | StepConstructionFailure   of StepHeader: IStepHeader  * Reasons: string list
        
type EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults> =
    {
        // Strictly speaking, for the interior data changes, we should use DataChangeSteps rather than UIDs within the map.
        // However, for the sake of acceptable pragmatism, the UID will work fine for our purposes.
        InteriorDataChanges : Map<Guid, 'TPolicyRecord>
        StepResults         : Map<Guid, StepDataSource * 'TStepResults>
    }

// Either all steps succeed, or we return a list of failures.
type EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults> =
    // This makes it explicit that we're only return internal data stages.
    Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, WalkEvaluationFailure list>


/// Required interface for any implementation that can
/// asyncronously evaluate a given policy.
// We can't replace this with a function signature as we require different inputs depending
// on the cohort to which the given policy belongs.
type IPolicyWalkEvaluator<'TPolicyRecord, 'TStepResults> =
    interface
        abstract member Execute:
            ExitedPolicy<'TPolicyRecord> * 'TStepResults option
                -> Task<EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

        abstract member Execute:
            ExitedPolicy<'TPolicyRecord> * 'TStepResults option * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart)
                -> Task<EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

        abstract member Execute:
            // We can optionally provide the prior closing step results.
            RemainingPolicy<'TPolicyRecord> * 'TStepResults option
                -> Task<EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

        abstract member Execute:
            RemainingPolicy<'TPolicyRecord> * 'TStepResults option * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart)
                -> Task<EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

        abstract member Execute:
            NewPolicy<'TPolicyRecord>
                -> Task<EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

        abstract member Execute:
            NewPolicy<'TPolicyRecord> * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart)
                -> Task<EvaluatedPolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>
    end
