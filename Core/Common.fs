
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
type StepDataSource =
    | OpeningData
    | ClosingData
    // Theoretically, we should use the data changer interface. However,
    // given it's generic with respect to the policy record type, we'd
    // then need to make this generic as well.
    | DataChangeStep of IStepHeader


[<NoEquality; NoComparison>]
type ExitedPolicy<'TPolicyRecord> =
    | ExitedPolicy of Opening: 'TPolicyRecord

[<NoEquality; NoComparison>]
type RemainingPolicy<'TPolicyRecord> =
    | RemainingPolicy of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord          
        
[<NoEquality; NoComparison>]
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
