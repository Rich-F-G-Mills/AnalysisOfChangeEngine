
namespace AnalysisOfChangeEngine.Implementations.Common

open AnalysisOfChangeEngine.Common


module StepTemplates =
    
    let opening_WithValidation<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (dataSource, validator)
        : OpeningStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Position"
                Description = "Represents prior closing position."
                DataSource = dataSource
                Validator = validator
            }

    let opening<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        dataSource =
            opening_WithValidation<'TPolicyRecord, 'TStepResults>
                (dataSource, fun _ -> List.empty)

    let openingRegression_WithValidation<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        validator
        : RegressionStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                Validator = validator
            }

    let openingRegression<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        openingRegression_WithValidation<'TPolicyRecord, 'TStepResults>
            (fun _ -> List.empty)

    let restatedOpeningData_WithValidation<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (dataChanges, validator) =
            InteriorStep<'TPolicyRecord, 'TStepResults>.DataChange {
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                DataChanges = dataChanges
                Validator = validator
            }

    let restatedOpeningData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        dataChanges =
            restatedOpeningData_WithValidation<'TPolicyRecord, 'TStepResults>
                (dataChanges, fun _ -> List.empty)

    let removeExited<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        : RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Remove Exited"
                Description = "Remove policies which have exited since the prior closing."
            }

    let restatedOpeningReturns_WithValidation<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        validator =
            InteriorStep<'TPolicyRecord, 'TStepResults>.ParameterChange {
                Title = "Restated Opening Returns"
                Description = "Restate the returns in the opening position."
                Validator = validator
            }

    let restatedOpeningReturns<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        restatedOpeningReturns_WithValidation<'TPolicyRecord, 'TStepResults>
            (fun _ -> List.empty)           

    let addNewRecords_WithValidation<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        validator
        : AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Add New Business/Reinstatements"
                Description = "Add new business and any policies which have reinstated."
                Validator = validator
            }

    let addNewRecords<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        addNewRecords_WithValidation<'TPolicyRecord, 'TStepResults>
            (fun _ -> List.empty)


