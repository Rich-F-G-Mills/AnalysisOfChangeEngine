
namespace AnalysisOfChangeEngine.ProviderImplementations


[<AutoOpen>]
module private Common =

    let (|Singleton|) = function
        | [ x ] -> x
        | _ -> failwith "Multiple elements provided."

