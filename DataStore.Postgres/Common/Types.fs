
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
            OpeningRunUid               : RunUid option
            ClosingRunDate              : DateOnly
            PolicyDataExtractionUid     : ExtractionUid        
        }

    [<NoEquality; NoComparison>]
    type StepHeader =
        {
            Uid                         : StepUid
            Title                       : string
            Description                 : string    
        }


    [<Sealed>]
    type PostgresProductSpecificEnumerationAttribute (typeName: string) =
        inherit Attribute()

        member val TypeName =
            typeName

    [<Sealed>]
    type PostgresCommonEnumerationAttribute (typeName: string) =
        inherit Attribute()

        member val TypeName =
            typeName
