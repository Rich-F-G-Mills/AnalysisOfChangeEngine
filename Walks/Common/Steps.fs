
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


type StepFactory<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
    (uidResolver: Guid -> string * string) =
        let uidResolver' uid =
            let title, description =
                uidResolver uid

            {|
                Uid = uid
                Title = title
                Description = description
            |}
  

        // Must be members in order to be generic.
        let makeSourceChangeStep
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

        let makeDataChangeStep
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

        member _.openingReRun
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

        member _.restatedOpeningData =
            makeDataChangeStep (Guid ("6d3385fc-8e01-4e95-9dc2-65471813ea5e"))

        member _.removeExited ()
            : RemoveExitedRecordsStep =
                let header =
                    uidResolver' (Guid ("6920fbd3-e1d1-4459-aca9-aa7c63267a14"))

                {
                    Uid = header.Uid
                    Title = header.Title
                    Description = header.Description
                }

        member _.aocOpeningConsistencyCheck =
            makeSourceChangeStep (Guid ("1254ec7b-e4ee-4a3b-b295-def984bc7e80"))

        member _.restatedOpeningAdjustments =
            makeSourceChangeStep (Guid ("7f0492ca-6211-4032-bf89-a15879d07856"))

        member _.restatedOpeningReturns =
            makeSourceChangeStep (Guid ("d3d411ec-f5d7-419a-a9ff-67e3d95a9e15"))

        member _.restatedOpeningDeductions =
            makeSourceChangeStep (Guid ("61530067-7402-435c-8c3e-933d80d19220"))

        member _.moveToClosingDate =
            makeSourceChangeStep (Guid ("12ff0e3a-eab8-4d55-94d5-92a561abd141"))

        member _.dataRollForward =
            makeDataChangeStep (Guid ("92ae5c91-e2f7-4112-8063-b143c467e4b3"))

        member _.adjustments =
            makeSourceChangeStep (Guid ("1b901498-b74d-408c-a3b4-f8654f63cf41"))

        member _.premiums =
            makeSourceChangeStep (Guid ("320d8985-c752-43e1-89ba-72411a2053ad"))

        member _.deductions =
            makeSourceChangeStep (Guid ("a1639a6e-9660-4f8f-8029-ce01d2767ce7"))

        member _.mortalityCharges =
            makeSourceChangeStep (Guid ("4eafa688-4ae0-4ded-aa92-2710f876a981"))           

        member _.investmentReturns =
            makeSourceChangeStep (Guid ("2df7f353-9b69-40c3-9f97-7b1fef2b8b2b"))

        member _.closingGuaranteedDeathBenefit =
            makeSourceChangeStep (Guid ("8d823d72-6027-49c0-8962-c1f725d199a5"))

        member _.closingDeathUpliftFactor =
            makeSourceChangeStep (Guid ("4698425d-e165-4881-a5dd-b90e342e1795"))

        member _.closingExitBonusRate =
            makeSourceChangeStep (Guid ("aff89bf5-604e-4989-8b11-c3ba24d1fca1"))

        member _.aocClosingConsistencyCheck =
            makeSourceChangeStep (Guid ("7e38d53b-960c-44e1-8adc-13bf645196ec"))

        member _.recentPaidUps =
            makeDataChangeStep (Guid ("b5c1db87-1bdd-4574-ad7a-86e4122c4799"))

        member _.moveToClosingData
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

        member _.moveToClosingMonthEnd =
            makeSourceChangeStep (Guid ("3f8c9b7e-2a4d-4f3a-9e2e-7d1c8a9f6b2c"))