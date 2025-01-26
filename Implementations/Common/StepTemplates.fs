
namespace AnalysisOfChangeEngine.Implementations.Common

open AnalysisOfChangeEngine.Common


module StepTemplates =
    
    let opening<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        dataSource
        : OpeningStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Position"
                Description = "Represents prior closing position."
                DataSource = dataSource
            }

    let openingRegression<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        validator
        : RegressionStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                Validator = validator
            }

    let restatedOpeningData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (dataChanges, validator)
        : DataChangeStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                DataChanges = dataChanges
                Validator = validator
            }

    let removeExited<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        toRemove
        : RemoveExitedStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Remove Exited"
                Description = "Remove policies which have exited since the prior closing."
                ToRemove = toRemove
            }

    let restatedOpeningReturns<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        validator
        : ParameterChangeStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Restated Opening Returns"
                Description = "Restate the returns in the opening position."
                Validator = validator
            }

    let addNewBusiness<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        toAdd
        : AddNewBusinessStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Add New Business/Reinstatements"
                Description = "Add new business and any policies which have reinstated."
                ToAdd = toAdd
            }


