
namespace AnalysisOfChangeEngine.Structures.StepResults


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    open AnalysisOfChangeEngine


    let inline private compareFloatWithTolerance tolerance (a: float32, b: float32) =        
        (-tolerance < a - b) && (a - b < tolerance)

    let private compareMonetaryValues =
        compareFloatWithTolerance 0.005f

    let private compareExitBonusRates =
        compareFloatWithTolerance 0.00001f

    let private compareDeathUpliftFactors =
        compareFloatWithTolerance 0.00001f


    // Other than our own fuzzy-comparison, we shouldn't be performing 
    // vanilla equality or comparison operations with this type.
    [<NoEquality; NoComparison>]
    type StepResults =
        {
            UnsmoothedAssetShare    : float32
            SmoothedAssetShare      : float32
            GuaranteedDeathBenefit  : float32
            SurrenderBenefit        : float32
            DeathBenefit            : float32
            ExitBonusRate           : float32
            UnpaidPremiums          : float32
            DeathUpliftFactor       : float32
        }

        member this.IsCloseTo (other: StepResults) =
            compareMonetaryValues (this.UnsmoothedAssetShare, other.UnsmoothedAssetShare) &&
            compareMonetaryValues (this.SmoothedAssetShare, other.SmoothedAssetShare) &&
            compareMonetaryValues (this.GuaranteedDeathBenefit, other.GuaranteedDeathBenefit) &&
            compareMonetaryValues (this.SurrenderBenefit, other.SurrenderBenefit) &&
            compareMonetaryValues (this.DeathBenefit, other.DeathBenefit) &&
            compareExitBonusRates (this.ExitBonusRate, other.ExitBonusRate) &&
            compareMonetaryValues (this.UnpaidPremiums, other.UnpaidPremiums) &&
            compareDeathUpliftFactors (this.DeathUpliftFactor, other.DeathUpliftFactor)
            