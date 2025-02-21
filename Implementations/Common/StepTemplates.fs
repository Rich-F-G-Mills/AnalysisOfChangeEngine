
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


    [<NoEquality; NoComparison>]
    type private StepHeader =
        {
            Title: string
            Description: string
        }

    let private makeRegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (stepHeader: StepHeader)
        (details: RegressionStepDetails<_, _, _>)
        : RegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                Title = stepHeader.Title
                Description = stepHeader.Description
                Source = details.Source
                Validator = details.Validator
            }

    let private makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (stepHeader: StepHeader)
        (details: ParameterChangeStepDetails<_, _, _>)
        : ParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            {
                Title = stepHeader.Title
                Description = stepHeader.Description
                Source = details.Source
                Validator = details.Validator
            }

    let private makeDataChangeStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (stepHeader: StepHeader)
        (details: DataChangeStepDetails<_, _>)
        : DataChangeStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = stepHeader.Title
                Description = stepHeader.Description
                DataChanger = details.DataChanger
                Validator = details.Validator
            }

    
    let opening<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: OpeningStepDetails<'TPolicyRecord, 'TStepResults>)
        : OpeningStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Opening Position"
                Description = "Represents prior closing position."
                Validator = details.Validator
            }

    let openingRegression<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeRegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Opening Re-Run"
            Description = "Re-run of prior closing data using latest model and assumptions."
        }

    let restatedOpeningData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        makeDataChangeStep<'TPolicyRecord, 'TStepResults> {
            Title = "Opening Re-Run"
            Description = "Re-run of prior closing data using latest model and assumptions."
        }

    let removeExited<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        : RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
            {
                Title = "Remove Exited"
                Description = "Remove policies which have exited since the prior closing."
            }

    let aocOpeningConsistencyCheck<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeRegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "AoC Opening Logic Consistency Check"
            Description = "Used to check consistency of AoC opening logic."
        }

    let restatedOpeningAdjustments<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Restated Opening Adjustments"
            Description = "Allow for restated openining adjustments (eg. mutual bonus)."
        }   

    let restatedOpeningReturns<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Restated Opening Returns"
            Description = "Restate the returns used in the opening position."
        }     
            
    let restatedOpeningDeductions<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Restated Opening Deductions"
            Description = "Restate the deductions used in the opening position."
        }
            
    let moveToClosingDate<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>=
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Move To Closing Run Date"
            Description = "Move from opening to closing run date."
        }

    let dataRollForward<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        makeDataChangeStep<'TPolicyRecord, 'TStepResults> {
            Title = "Data Roll-Forward"
            Description = "Allow for actual roll-forward of data (eg. NPDDs)."
        }

    let adjustments<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Adjustments"
            Description = "Allow for adjustments over the period."
        }

    let premiums<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Premiums"
            Description = "Allow for premiums over the period."
        }

    let deductions<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Deductions"
            Description = "Allow for deductions over the period such as AMCs and IMCs."
        }

    let mortalityCharges<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Mortality Charges"
            Description = "Allow for mortality charges over the period."
        }

    let investmentReturns<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Investment Returns"
            Description = "Allow for investment returns over the period."
        }

    let closingGuaranteedDeathBenefit<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Closing Guaranteed Death Benefit"
            Description = "Allow for closing guaranteed death benefit."
        }

    let closingDeathUpliftFactor<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Closing Death Uplift Factor"
            Description = "Allow for closing death uplift factor."
        }

    let closingExitBonusRate<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeParameterChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "Closing Exit Bonus Rate"
            Description = "Allow for closing death uplift factor."
        }

    let aocClosingConsistencyCheck<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        makeRegressionStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> {
            Title = "AoC Closing Logic Consistency Check"
            Description = "Used to check consistency of AoC closing logic."
        }

    let recentPaidUps<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        makeDataChangeStep<'TPolicyRecord, 'TStepResults> {
            Title = "Status Changes To Paid-Up"
            Description = "Allow for statuses transitioning to PUP."
        }

    let moveToClosingExistingData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (details: ClosingExistingDataStepDetails<'TPolicyRecord, 'TStepResults>)
        : ClosingExistingDataStep<'TPolicyRecord, 'TStepResults> =
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