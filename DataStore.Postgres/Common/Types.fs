
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module Types =

    open System
    open AnalysisOfChangeEngine


    [<NoEquality; NoComparison>]
    type ExtractionUid =
        | ExtractionUid of Guid

        member this.Value =
            match this with
            | ExtractionUid uid -> uid

    [<NoEquality; NoComparison>]
    type SessionUid =
        | SessionUid of Guid

        member this.Value =
            match this with
            | SessionUid uid -> uid

    
    [<NoEquality; NoComparison>]
    type SessionContext =
        {
            UserName    : string
        }

    [<NoEquality; NoComparison>]
    type ExtractionHeader =
        {
            Uid                         : ExtractionUid
            ExtractionDate              : DateOnly
        }

    [<NoEquality; NoComparison>]
    type RunHeader =
        {
            Uid                         : RunUid
            Title                       : string
            Comments                    : string option
            CreatedBy                   : string
            CreatedWhen                 : DateTime
            PriorRunUid                 : RunUid option
            ClosingRunDate              : DateOnly
            PolicyDataExtractionUid     : ExtractionUid        
        }

    [<NoEquality; NoComparison>]
    type StepHeader =
        {
            Uid                         : StepUid
            Title                       : string
            Description                 : string   
            RunIfExitedRecord           : bool
            RunIfNewRecord              : bool
        }


    // These should hopefully minimize chance of these getting mixed up.
    [<NoEquality; NoComparison>]
    type internal ProductSchemaName =
        ProductSchemaName of string

    [<NoEquality; NoComparison>]
    type internal EnumSchemaName =
        EnumSchemaName of string

    [<NoEquality; NoComparison>]
    type internal PgTypeName =
        PgTypeName of string


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type PostgresEnumerationSchema =
        | Common
        | ProductSpecific

    [<AbstractClass>]
    [<AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)>]
    type PostgresEnumerationAttribute (pgTypeName: string, location: PostgresEnumerationSchema) =
        inherit Attribute ()

        member val PgTypeName =
            pgTypeName with get

        member val Location =
            location with get

    [<Sealed>]
    type PostgresProductSpecificEnumerationAttribute (typeName: string) =
        inherit PostgresEnumerationAttribute (typeName, PostgresEnumerationSchema.ProductSpecific)

    [<Sealed>]
    type internal PostgresCommonEnumerationAttribute (typeName: string) =
        inherit PostgresEnumerationAttribute (typeName, PostgresEnumerationSchema.Common)
