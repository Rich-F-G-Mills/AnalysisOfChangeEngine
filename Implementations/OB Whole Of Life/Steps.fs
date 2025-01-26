
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
        ExitedPolicies: Set<PolicyID>
        ReinstatedPolicies: Set<PolicyID>
        OpeningDataSource: IPolicyRecordSource<PolicyRecord>        
        RestatedDataChanges: Map<PolicyID, PolicyRecord>        
        ClosingDataSource: IPolicyRecordSource<PolicyRecord>
    }


type Walk private (logger: ILogger, config: WalkConfiguration) as this =
    inherit AbstractWalk<PolicyRecord, StepResults> (logger, "OB Whole-Life")

    // --- VALIDATE THE SUPPLIED CONFIGURATION ---

    static member create logger (config: WalkConfiguration) =
        result {
            let intersection =
                Set.intersect config.ExitedPolicies config.ReinstatedPolicies

            do! intersection 
                |> Result.requireEmpty "Cannot have policies that both exit and reinstate!"

            return new Walk (logger, config)
        }
        
    // --- DEFINE THE INDIVIDUAL STEPS ---

    member val opening =
        StepTemplates.opening config.OpeningDataSource

    member val openingRegression =
        StepTemplates.openingRegression (fun (_, before, after) ->
                if before = after then
                    List.empty
                else
                    [ StepValidationIssue.Warning "Regression mis-match" ]
            )

    member val removeExited =
        StepTemplates.removeExited config.ExitedPolicies
        

    member val addNewBusiness =
        StepTemplates.addNewBusiness config.ReinstatedPolicies

