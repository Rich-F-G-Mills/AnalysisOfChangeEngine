
namespace AnalysisOfChangeEngine.Implementations.OBWholeOfLife

open FsToolkit.ErrorHandling
open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.Implementations.Common


[<NoComparison>]
type StepResults =
    {
        UnsmoothedAssetShare: double
        SmoothedAssetShare: double
        GuaranteedDeathBenefit: double
        SurrenderValue: double
        DeathBenefit: double
        ExitBonusRate: double
        UnpaidPremiums: double
    }


[<NoEquality; NoComparison>]
type WalkConfiguration =
    {
        OpeningDataSource: IPolicyRecordSource<PolicyRecord>        
        RestatedOpeningDataChanges: Map<PolicyID, PolicyRecord>   
        RollForwardDataChanges: Map<PolicyID, PolicyRecord>
        RecentPUPs: Map<PolicyID, PolicyRecord>
        ClosingDataSource: IPolicyRecordSource<PolicyRecord>
    }


type Walk private (logger: ILogger, config: WalkConfiguration) as this =
    inherit AbstractWalk<PolicyRecord, StepResults> (
        logger,
        "OB Whole-Life",
        config.OpeningDataSource,
        config.ClosingDataSource
    )

    // --- VALIDATE THE SUPPLIED CONFIGURATION ---

    static member create logger (config: WalkConfiguration) =
        result {
            return new Walk (logger, config)
        }


    // ...REQUIRED...
    override val opening =
        StepTemplates.opening config.OpeningDataSource

    // ...REQUIRED...
    override val openingRegression =
        StepTemplates.openingRegression_WithValidation (
            fun (_, before, after) ->
                if before = after then
                    List.empty
                else
                    [ StepValidationIssue.Warning "Regression mis-match" ]
        )

    // ...REQUIRED...
    override val removeExitedRecords =
        StepTemplates.removeExited 


    // --- PRODUCT SPECIFIC STEPS ---
    // We need to make sure that these are registered as well as defined!

    member val restatedOpeningData =
        this.registerInteriorStep(
            StepTemplates.restatedOpeningData config.RestatedOpeningDataChanges
        )        

    member val restatedOpeningReturns =
        this.registerInteriorStep(
            StepTemplates.restatedOpeningReturns
        )      

 

    // ...REQUIRED...
    override val addNewRecords =
        StepTemplates.addNewRecords

