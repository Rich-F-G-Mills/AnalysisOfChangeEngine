
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module TypeExtensions =

    open System


    [<RequireQualifiedAccess>]
    module internal String =

        // Useful when generating SQL queries.
        let join delim (xs: #seq<string>) =
            String.Join (delim, xs)


    [<RequireQualifiedAccess>]
    module SqlValue =

        let private makeNullable fnSqlValue = function
            | Some value -> fnSqlValue value
            | None       -> SqlValue.Null            

        let StringOrNull = 
            makeNullable SqlValue.String

        let UuidOrNull = 
            makeNullable SqlValue.Uuid


    // Copied over from Common project.
    [<RequireQualifiedAccess>]
    module Option =        
        let requireTrue = function
            | true -> Some ()
            | false -> None