
namespace AnalysisOfChangeEngine


    open System
    open System.Threading.Tasks


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type PolicyGetterFailure =
        /// Indicates that, although the required record was found, it could
        /// not be successfully parsed into a corresponding policy record object.
        | ParseFailure of Reasons: string array
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

    type IStepResultsGetter<'TStepResults> =
        interface
            /// Only records which could be successfully found will be included in the map.
            abstract member GetStepResultsAsync :
                policyIds : string array
                    -> Task<Map<string, 'TStepResults>>
        end


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

    
    /// Required interface for any implementation that can
    /// asyncronously evaluate a given policy.
    // We can't replace this with a function signature as we require different inputs depending
    // on the cohort to which the given policy belongs.
    type IPolicyEvaluator<'TPolicyRecord, 'TStepResults> =
        interface
            abstract member Execute:
                ExitedPolicy<'TPolicyRecord> * 'TStepResults option
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

            abstract member Execute:
                ExitedPolicy<'TPolicyRecord> * 'TStepResults option * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart)
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

            abstract member Execute:
                // We can optionally provide the prior closing step results.
                RemainingPolicy<'TPolicyRecord> * 'TStepResults option
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

            abstract member Execute:
                RemainingPolicy<'TPolicyRecord> * 'TStepResults option * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart)
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

            abstract member Execute:
                NewPolicy<'TPolicyRecord>
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>

            abstract member Execute:
                NewPolicy<'TPolicyRecord> * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart)
                    -> Task<PolicyWalkOutcome<'TPolicyRecord, 'TStepResults>>
        end