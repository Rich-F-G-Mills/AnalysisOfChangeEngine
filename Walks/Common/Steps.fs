
namespace AnalysisOfChangeEngine.Walks.Common.Steps

open AnalysisOfChangeEngine


[<NoEquality; NoComparison>]
type OpeningReRunStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    {
        Source: OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        Validator: OpeningReRunStepValidator<'TPolicyRecord, 'TStepResults>
    }

[<NoEquality; NoComparison>]
type SourceChangeStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    {
        Source: SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        Validator: SourceChangeStepValidator<'TPolicyRecord, 'TStepResults>
    }

[<NoEquality; NoComparison>]
type DataChangeStepDetails<'TPolicyRecord, 'TStepResults> =
    {
        DataChanger: PolicyRecordChanger<'TPolicyRecord>
        Validator: DataChangeStepValidator<'TPolicyRecord, 'TStepResults>
    }

[<NoEquality; NoComparison>]
type MoveToClosingDataStepDetails<'TPolicyRecord, 'TStepResults> =
    {
        Validator: DataChangeStepValidator<'TPolicyRecord, 'TStepResults>
    }

[<NoEquality; NoComparison>]
type AddNewRecordsStepDetails<'TPolicyRecord, 'TStepResults> =
    {
        Validator: AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults>
    }


namespace AnalysisOfChangeEngine.Walks.Common

open System
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Walks.Common.Steps


type StepFactory (uidResolver: Guid -> string * string) as this =
    let uidResolver' uid =
        let title, description =
            uidResolver uid

        {|
            Uid = uid
            Title = title
            Description = description
        |}
  

    // Must be members in order to be generic.
    member private _.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (uid: Guid)
        (details: SourceChangeStepDetails<_, _, _>)
        : SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            let header =
                uidResolver' uid

            {
                Uid = uid
                Title = header.Title
                Description = header.Description
                Source = details.Source
                Validator = details.Validator
            }

    member private _.makeDataChangeStep<'TPolicyRecord, 'TStepResults>
        uid (details: DataChangeStepDetails<_, _>)
        : DataChangeStep<'TPolicyRecord, 'TStepResults> =
            let header =
                uidResolver' uid

            {
                Uid = uid
                Title = header.Title
                Description = header.Description
                DataChanger = details.DataChanger
                Validator = details.Validator
            }

    member _.openingReRun<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (details: OpeningReRunStepDetails<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        : OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            let header =
                uidResolver' (Guid ("f198c564-7f6b-46a7-af3c-bd9b4ef6bc1b"))

            {
                Uid = header.Uid
                Title = header.Title
                Description = header.Description
                Source = details.Source
                Validator = details.Validator
            }

    member _.restatedOpeningData<'TPolicyRecord, 'TStepResults> details =
        this.makeDataChangeStep<'TPolicyRecord, 'TStepResults>
            (Guid ("6d3385fc-8e01-4e95-9dc2-65471813ea5e")) details

    member _.removeExited<'TPolicyRecord, 'TStepResults> ()
        : RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
            let header =
                uidResolver' (Guid ("6920fbd3-e1d1-4459-aca9-aa7c63267a14"))

            {
                Uid = header.Uid
                Title = header.Title
                Description = header.Description
            }

    member _.aocOpeningConsistencyCheck<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("1254ec7b-e4ee-4a3b-b295-def984bc7e80")) details

    member _.restatedOpeningAdjustments<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("7f0492ca-6211-4032-bf89-a15879d07856")) details

    member _.restatedOpeningReturns<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("d3d411ec-f5d7-419a-a9ff-67e3d95a9e15")) details

    member _.restatedOpeningDeductions<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("61530067-7402-435c-8c3e-933d80d19220")) details

    member _.moveToClosingDate<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("12ff0e3a-eab8-4d55-94d5-92a561abd141")) details

    member _.dataRollForward<'TPolicyRecord, 'TStepResults> details =
        this.makeDataChangeStep<'TPolicyRecord, 'TStepResults>
            (Guid ("92ae5c91-e2f7-4112-8063-b143c467e4b3")) details

    member _.adjustments<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("1b901498-b74d-408c-a3b4-f8654f63cf41")) details

    member _.premiums<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("320d8985-c752-43e1-89ba-72411a2053ad")) details

    member _.deductions<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("a1639a6e-9660-4f8f-8029-ce01d2767ce7")) details

    member _.mortalityCharges<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("4eafa688-4ae0-4ded-aa92-2710f876a981")) details            

    member _.investmentReturns<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("2df7f353-9b69-40c3-9f97-7b1fef2b8b2b")) details

    member _.closingGuaranteedDeathBenefit<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("8d823d72-6027-49c0-8962-c1f725d199a5")) details

    member _.closingDeathUpliftFactor<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("4698425d-e165-4881-a5dd-b90e342e1795")) details

    member _.closingExitBonusRate<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("aff89bf5-604e-4989-8b11-c3ba24d1fca1")) details

    member _.aocClosingConsistencyCheck<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("7e38d53b-960c-44e1-8adc-13bf645196ec")) details

    member _.recentPaidUps<'TPolicyRecord, 'TStepResults> details =
        this.makeDataChangeStep<'TPolicyRecord, 'TStepResults>
            (Guid ("b5c1db87-1bdd-4574-ad7a-86e4122c4799")) details

    member _.moveToClosingData<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality>
        (details: MoveToClosingDataStepDetails<_, _>)
        : MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> =
            let header =
                uidResolver' (Guid ("ba0238f1-df2b-43c7-95b2-557d846a81f8"))

            {
                Uid = header.Uid
                Title = header.Title
                Description = header.Description
                Validator = details.Validator
            }

    member _.addNewRecords<'TPolicyRecord, 'TStepResults>
        (details: AddNewRecordsStepDetails<_, _>)
        : AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
            let header =
                uidResolver' (Guid ("cf93808c-bacc-4137-b849-6227df8c177f"))

            {
                Uid = header.Uid
                Title = header.Title
                Description = header.Description
                Validator = details.Validator
            }

    member _.moveToClosingMonthEnd<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("3f8c9b7e-2a4d-4f3a-9e2e-7d1c8a9f6b2c")) details

    member _.switchToValuationAssetShares<'TPolicyRecord, 'TStepResults, 'TApiCollection> details =
        this.makeSourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            (Guid ("9a1d4c3f-6b7e-4d2a-8f9c-3e2b1a7d5c4e")) details
