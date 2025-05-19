
namespace AnalysisOfChangeEngine.Structures.StepResults


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    // Retain the compiler generated structural equality as used later.
    [<NoComparison>]
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