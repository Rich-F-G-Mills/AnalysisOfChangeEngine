
namespace AnalysisOfChangeEngine

open System


type ILogger =
    interface
        abstract member LogInfo     : message: string -> unit
        abstract member LogDebug    : message: string -> unit
        abstract member LogWarning  : message: string -> unit
        abstract member LogError    : message: string -> unit
    end


/// Required interface for all step types.
type IStepHeader =
    interface
        abstract member Uid         : Guid with get
        abstract member Title       : string with get
        abstract member Description : string with get
    end


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
/// Specifies the policy data used to run a particular step.
type StepDataSource =
    /// Indicates that the opening policy data was used without any subsequent changes.
    | OpeningData
    /// Indicates that the closing policy data was used. By definition, this could not
    /// reflect any subsequent changes.
    | ClosingData
    /// Indicates that policy data as constructed by the current or previous data change
    /// step is being used.
    // Theoretically, we should use the data changer interface. However,
    // given it's generic with respect to the policy record type, we'd
    // then need to make this generic as well.
    | DataChangeStep of IStepHeader


(*
Design decision: Why bother with the following wrappers?
    The idea is to make it very explicit what kind of record
    we're dealing with. This should help avoid mistakes
    where, for example, an exited record is accidentally
    treated as a new record. As such, they are used by
    (for example) evaluators to ensure that the right steps
    are being applied depending on the cohort of the policy
    in question.
*)


[<NoEquality; NoComparison>]
/// Used to wrap the details of an exited record.
type ExitedPolicy<'TPolicyRecord> =
    | ExitedPolicy of Opening: 'TPolicyRecord

[<NoEquality; NoComparison>]
/// Used to wrap the details of a policy present in both the opening and closing data sets.
type RemainingPolicy<'TPolicyRecord> =
    | RemainingPolicy of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord          
        
[<NoEquality; NoComparison>]
/// Used to wrap the details of a new record.
type NewPolicy<'TPolicyRecord> =
    | NewPolicy of Closing: 'TPolicyRecord


[<NoEquality; NoComparison>]
type ExitedPolicyId =
    | ExitedPolicyId of string

[<NoEquality; NoComparison>]
type RemainingPolicyId =
    | RemainingPolicyId of string

[<NoEquality; NoComparison>]
type NewPolicyId =
    | NewPolicyId of string
