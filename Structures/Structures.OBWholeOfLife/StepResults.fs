
namespace AnalysisOfChangeEngine.Structures.StepResults


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    // Retain the compiler generated structural equality as used later.
    [<NoComparison>]
    type StepResults =
        {
            UnsmoothedAssetShare    : double
            SmoothedAssetShare      : double
            GuaranteedDeathBenefit  : double
            SurrenderValue          : double
            DeathBenefit            : double
            ExitBonusRate           : double
            UnpaidPremiums          : double
            DeathUpliftFactor       : double
        }