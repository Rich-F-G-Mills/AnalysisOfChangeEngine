
namespace AnalysisOfChangeEngine

open System.Collections.Generic


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

