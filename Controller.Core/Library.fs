
namespace AnalysisOfChangeEngine.Controller


[<AutoOpen>]
module Core =

    open System
    open System.Threading.Tasks
    open AnalysisOfChangeEngine

    (*
    Design Decision:
        Why doesn't all of this live within the top-level Core project?
        Again, more of a philisophical decision, but I decides that anything
        particular to the controller itself would sit outside of the top-level
        Core logic. Reason being is that there are a number of ways such a controller
        could actually be implemented. It didn't make sense to constrain it in any way.
        Arguably, the same "thinking" was applied to the location of telemtry related classes.
    *)


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type PolicyGetterFailure =
        /// Indicates that, although the required record was found, it could
        /// not be successfully parsed into a corresponding policy record object.
        | ParseFailure of Reasons: string list
        /// No record could be found with the requisite ID.
        | NotFound

    type PolicyGetterOutcome<'TPolicyRecord> =
        Result<'TPolicyRecord, PolicyGetterFailure>

    // We could just use a simple function signature here. However,
    // if this needs to be disposable, easier if it's an interface.
    type IPolicyGetter<'TPolicyRecord> =
        interface
            /// Only records which could be successfully found will be included in the map.
            abstract member GetPolicyRecordsAsync :
                policyIds : string array
                    -> Task<Map<string, PolicyGetterOutcome<'TPolicyRecord>>>
        end


    // As above, using an interface allows IDisposable to be implemented if needed.
    type IStepResultsGetter<'TStepResults> =
        interface
            /// Only records which could be successfully found will be included in the map.
            abstract member GetStepResultsAsync :
                policyIds : string array
                    -> Task<Map<string, 'TStepResults>>
        end

    
    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type PolicyReadFailure =
        | OpeningRecordNotFound
        | ClosingRecordNotFound
        | OpeningRecordParseFailure of Reasons: string list
        | ClosingRecordParseFailure of Reasons: string list

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type PolicyWriteFailure =
        // We don't have sight of the step header at this point.
        // We'll have to settle for just the UID.
        | DataStageWriteFailure     of DataStageUid: Guid * Reasons: string list 
        | StepResultsWriteFailure   of StepUid: Guid * Reasons: string list

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type ProcessedPolicyFailure =
        // If we've got read failures, then we won't have actually evaluated anything!
        | ReadFailures of PolicyReadFailure list
        | EvaluationFailures of WalkEvaluationFailure list
        // There can only be one of these failures.
        | PolicyWriteFailure of PolicyWriteFailure

    [<NoEquality; NoComparison>]
    type ProcessedPolicyOutcome<'TPolicyRecord, 'TStepResults> =
        {
            PolicyId    : string
            WalkOutcome : Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, ProcessedPolicyFailure>
        }

    type IProcessedOutputWriter<'TPolicyRecord, 'TStepResults> =
        interface
            /// Only records which could be successfully found will be included in the map.
            abstract member WriteProcessedOutputAsync :
                ProcessedPolicyOutcome<'TPolicyRecord, 'TStepResults> array
                    -> Task<Unit>
        end