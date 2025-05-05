
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module TypeExtensions =

    [<RequireQualifiedAccess>]
    module SqlValue =

        let private makeNullable fnSqlValue = function
            | Some value -> fnSqlValue value
            | None       -> SqlValue.Null
            

        let StringOrNull = 
            makeNullable SqlValue.String

        let UuidOrNull = 
            makeNullable SqlValue.Uuid