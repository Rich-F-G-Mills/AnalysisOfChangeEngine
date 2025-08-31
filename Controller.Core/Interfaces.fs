
namespace AnalysisOfChangeEngine.Controller

open System.Threading.Tasks
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Common


(*
Design Decision:
    Why doesn't all of this live within the top-level Core project?
    Again, more of a philisophical decision, but I decided that anything
    particular to the controller itself would sit outside of the top-level
    Core logic. Reason being is that there are a number of ways such a controller
    could actually be implemented. It didn't make sense to constrain it in any way.
    Arguably, the same "thinking" was applied to the location of telemtry related classes.
*)

type PolicyGetterOutcome<'TPolicyRecord> =
    Result<'TPolicyRecord, string nonEmptyList>

// We could just use a simple function signature here. However,
// if this needs to be disposable, easier if it's an interface.
type IPolicyGetter<'TPolicyRecord> =
    interface
        /// Only records which could be successfully found will be included in the map.
        abstract member GetPolicyRecordsAsync :
            policyIds : string array
                -> Task<Map<string, PolicyGetterOutcome<'TPolicyRecord>>>
    end


type StepResultsGetterOutcome<'TStepResults> =
    Result<'TStepResults, string nonEmptyList>

// As above, using an interface allows IDisposable to be implemented if needed.
type IStepResultsGetter<'TStepResults> =
    interface
        /// Only records which could be successfully found will be included in the map.
        abstract member GetStepResultsAsync :
            policyIds : string array
                -> Task<Map<string, StepResultsGetterOutcome<'TStepResults>>>
    end

    
[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type PolicyReadFailure =
    | OpeningRecordNotFound
    | ClosingRecordNotFound
    | OpeningRecordReadFailure             of Reasons: string nonEmptyList
    | ClosingRecordReadFailure             of Reasons: string nonEmptyList
    | PriorClosingStepResultsReadFailure   of Reasons: string nonEmptyList

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type ProcessedPolicyFailure =
    // If we've got read failures, then we won't have actually evaluated anything!
    | ReadFailures of PolicyReadFailure nonEmptyList
    | EvaluationFailures of WalkEvaluationFailure nonEmptyList

[<NoEquality; NoComparison>]
type ProcessedPolicyOutcome<'TPolicyRecord, 'TStepResults> =
    {
        PolicyId    : string
        WalkOutcome : Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, ProcessedPolicyFailure>
    }


[<NoEquality; NoComparison>]
type WrittenOutputOutcome =
    {
        /// Indicates whether any failures have arisen SPECIFICALLY during the write process.
        FailuresGenerated   : bool
    }

type IProcessedOutputWriter<'TPolicyRecord, 'TStepResults> =
    interface
        /// For each array element provided, this MUST provide a corresponding element in the output array.
        abstract member WriteProcessedOutputAsync :
            ProcessedPolicyOutcome<'TPolicyRecord, 'TStepResults> array
                -> Task<WrittenOutputOutcome array>
    end
