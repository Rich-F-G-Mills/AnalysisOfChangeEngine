
namespace AnalysisOfChangeEngine.Controller.CalculationLoop

open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal CohortedPolicyId =
    | Exited    of string
    | Remaining of string
    | New       of string

    member this.Underlying =
        match this with
        | Exited pid    -> pid
        | Remaining pid -> pid
        | New pid       -> pid

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal CohortedPolicyRecord<'TPolicyRecord, 'TStepResults> =
    | Exited    of Record: 'TPolicyRecord * PriorClosingResults: 'TStepResults option
    | Remaining of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord * PriorClosingResults: 'TStepResults option
    | New       of 'TPolicyRecord

[<NoEquality; NoComparison>]
type internal PolicyEvaluationRequest<'TPolicyRecord, 'TStepResults> =
    {
        PolicyId            : CohortedPolicyId
        PolicyRecord        : Result<CohortedPolicyRecord<'TPolicyRecord, 'TStepResults>, PolicyReadFailure nonEmptyList>
    }

[<NoEquality; NoComparison>]
type internal OutputFailedPolicyReadRequest =
    {
        PolicyId            : CohortedPolicyId
        FailureReasons      : PolicyReadFailure nonEmptyList
    }

[<NoEquality; NoComparison>]
type internal OutputCompletedEvaluationRequest<'TPolicyRecord, 'TStepResults> =
    {
        PolicyId            : CohortedPolicyId
        WalkOutcome         : Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, WalkEvaluationFailure nonEmptyList>
    }

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal OutputRequest<'TPolicyRecord, 'TStepResults> =
    | CompletedEvaluation   of OutputCompletedEvaluationRequest<'TPolicyRecord, 'TStepResults>
    | FailedPolicyRead      of OutputFailedPolicyReadRequest
