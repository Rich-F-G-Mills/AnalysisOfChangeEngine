
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module TypeExtensions =

    open System


    [<RequireQualifiedAccess>]
    module internal String =

        // Useful when generating SQL queries.
        let join delim (xs: #seq<string>) =
            String.Join (delim, xs)


    // Copied over from Common project.
    [<RequireQualifiedAccess>]
    module Option =        
        let requireTrue = function
            | true -> Some ()
            | false -> None

        let requireFalse = function
            | true -> None
            | false -> Some ()
