
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
            RunUid                      : RunUid
            Title                       : string
            Comments                    : string option
            CreatedBy                   : string
            CreatedWhen                 : DateTime
            PriorRunUid                 : RunUid option
            ClosingRunDate              : DateOnly
            PolicyDataExtractionUid     : ExtractionUid
            // Given you can't have a closing step UID without the run itself,
            // it made more sense for a given run to specify its closing step rather
            // than have it imposed upon it my the subsequent run.
            ClosingStepUid              : StepUid
        }

    [<NoEquality; NoComparison>]
    type StepHeader =
        {
            StepUid                     : StepUid
            Title                       : string
            Description                 : string   
            RunIfExitedRecord           : bool
            RunIfNewRecord              : bool
        }


    // These should hopefully minimize chance of these getting mixed up.
    [<NoEquality; NoComparison>]
    type internal PgTypeName =
        PgTypeName of string


    [<AbstractClass>]
    [<AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)>]
    type PostgresEnumerationAttribute (pgTypeName: string, schema: string) =
        inherit Attribute ()

        member val PgTypeName =
            pgTypeName with get

        member val Schema =
             schema with get


    [<Sealed>]
    type internal PostgresCommonEnumerationAttribute (typeName: string) =
        inherit PostgresEnumerationAttribute (typeName, "common")
