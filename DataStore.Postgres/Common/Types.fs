
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module Types =

    open System
    

    (*
    Design Decision:
        Why would we need these you ask?
        Given that the Guid type is used throughout, it's not impossible that (for example) an
        extraction uid suddently finds itself being used a run uid. This should (!) reduce the
        chance of that happening.
        Why not put these in Common? Because other implementations may not even use Guids to
        locate items within a datastore.
    *)
    type RunUid =
        | RunUid of Guid

        member this.Value =
            match this with
            | RunUid uid -> uid

    type ExtractionUid =
        | ExtractionUid of Guid

        member this.Value =
            match this with
            | ExtractionUid uid -> uid

    type StepUid =
        | StepUid of Guid

        member this.Value =
            match this with
            | StepUid uid -> uid

    
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

    // Implement equality logic so we can use GroupBy.
    [<RequireQualifiedAccess; NoComparison>]
    type CohortMembership =
        | Exited
        | Remaining
        | New

    [<NoEquality; NoComparison>]
    type OutstandingRecord =
        {
            PolicyId                    : string
            HasRunError                 : bool
            Cohort                      : CohortMembership
        }

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
    [<AttributeUsage(validOn = AttributeTargets.Class, AllowMultiple = false)>]
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