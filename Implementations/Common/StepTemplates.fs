
namespace AnalysisOfChangeEngine.Implementations.Common


module StepTemplates =

    open FSharp.Quotations

    open AnalysisOfChangeEngine.Common


    [<NoEquality; NoComparison>]
    type OpeningStepDetails<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Validator: OpeningStepValidator<'TPolicyRecord, 'TStepResults>
        }

    [<NoEquality; NoComparison>]
    type RegressionStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        {
            Source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator: RegressionValidator<'TPolicyRecord, 'TStepResults>
        }

    [<NoEquality; NoComparison>]
    type DataChangeStepDetails<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            DataChanger: PolicyRecordChanger<'TPolicyRecord>
            Validator: DataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

    [<NoEquality; NoComparison>]
    type ParameterChangeStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        {
            Source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            Validator: ParameterChangeValidator<'TPolicyRecord, 'TStepResults>
        }

    [<NoEquality; NoComparison>]
    type MoveToClosingExistingDataStepDetails<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Validator: DataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

    [<NoEquality; NoComparison>]
    type AddNewRecordsStepDetails<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Validator: AddNewRecordsValidator<'TPolicyRecord, 'TStepResults>
        }

    
    let opening<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: OpeningStepDetails<'TPolicyRecord, 'TStepResults>)
        : OpeningStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Position"
                Description = "Represents prior closing position."
                Validator = details.Validator
            }

    let openingRegression<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: RegressionStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                Source = details.Source
                Validator = details.Validator
            }

    let restatedOpeningData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: DataChangeStepDetails<'TPolicyRecord, 'TStepResults>)
        : DataChangeStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                DataChanger = details.DataChanger
                Validator = details.Validator
            }

    let removeExited<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        : RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Remove Exited"
                Description = "Remove policies which have exited since the prior closing."
            }

    let aocOpeningConsistencyCheck<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: RegressionStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                Title = "AoC Opening Logic Consistency Check"
                Description = "Used to check consistency of AoC opening logic."
                Source = details.Source
                Validator = details.Validator
            }

    let restatedOpeningReturns<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: ParameterChangeStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                Title = "Restated Opening Returns"
                Description = "Restate the returns used in the opening position."
                Source = details.Source
                Validator = details.Validator
            }     
            
    let restatedOpeningDeductions<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: ParameterChangeStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                Title = "Restated Opening Deductions"
                Description = "Restate the deductions used in the opening position."
                Source = details.Source
                Validator = details.Validator
            } 

    let moveToClosingExistingData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: MoveToClosingExistingDataStepDetails<'TPolicyRecord, 'TStepResults>)
        : MoveToClosingExistingDataStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Closing Data Regression"
                Description = "Regression check that closing data has been used for existing business."
                Validator = details.Validator
            }

    let addNewRecords<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: AddNewRecordsStepDetails<'TPolicyRecord, 'TStepResults>)
        : AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Add New Business/Reinstatements"
                Description = "Add new business and any policies which have reinstated."
                Validator = details.Validator
            }
