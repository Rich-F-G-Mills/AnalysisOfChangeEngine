
namespace AnalysisOfChangeEngine.Implementations


module StepTemplates =

    open System
    open FSharp.Quotations

    open AnalysisOfChangeEngine


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
    type ClosingExistingDataStepDetails<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
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
                //Uid = Guid("07bf48fa-dcf9-4b54-959d-18dc9174c185")
                Title = "Opening Position"
                Description = "Represents prior closing position."
                Validator = details.Validator
            }

    let openingRegression<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: RegressionStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                //Uid = Guid("4ad51a08-d8c1-40f3-b583-de5faffa680e")
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                Source = details.Source
                Validator = details.Validator
            }

    let restatedOpeningData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: DataChangeStepDetails<'TPolicyRecord, 'TStepResults>)
        : DataChangeStep<'TPolicyRecord, 'TStepResults> =
            {
                //Uid = Guid("3f558b2e-90f7-405a-9575-8c123ac0d94d")
                Title = "Opening Re-Run"
                Description = "Re-run of prior closing data using latest model and assumptions."
                DataChanger = details.DataChanger
                Validator = details.Validator
            }

    let removeExited<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        : RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                //Uid = Guid("6488ccf8-15fb-45d4-bc57-c69c331b8715")
                Title = "Remove Exited"
                Description = "Remove policies which have exited since the prior closing."
            }

    let aocOpeningConsistencyCheck<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: RegressionStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                //Uid = Guid("57856aaf-7478-4804-9f2d-d365a29ada12")
                Title = "AoC Opening Logic Consistency Check"
                Description = "Used to check consistency of AoC opening logic."
                Source = details.Source
                Validator = details.Validator
            }

    let restatedOpeningReturns<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: ParameterChangeStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                //Uid = Guid("ead97058-8281-4a5a-8ecc-c5eb9147ade6")
                Title = "Restated Opening Returns"
                Description = "Restate the returns used in the opening position."
                Source = details.Source
                Validator = details.Validator
            }     
            
    let restatedOpeningDeductions<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (details: ParameterChangeStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                //Uid = Guid("6841bc71-49b9-4950-a9e2-8c9b4f3ac75a")
                Title = "Restated Opening Deductions"
                Description = "Restate the deductions used in the opening position."
                Source = details.Source
                Validator = details.Validator
            } 

    let moveToClosingExistingData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: ClosingExistingDataStepDetails<'TPolicyRecord, 'TStepResults>)
        : ClosingExistingDataStep<'TPolicyRecord, 'TStepResults> =
            {
                //Uid = Guid("fd3861d2-e1b8-47ee-a59c-bc4954a6db62")
                Title = "Closing Data Regression"
                Description = "Regression check that closing data has been used for existing business."
                Validator = details.Validator
            }

    let addNewRecords<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: AddNewRecordsStepDetails<'TPolicyRecord, 'TStepResults>)
        : AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                //Uid = Guid("05c1de53-88b1-4357-8a22-8f9734f58159")
                Title = "Add New Business/Reinstatements"
                Description = "Add new business and any policies which have reinstated."
                Validator = details.Validator
            }
