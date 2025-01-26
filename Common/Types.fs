
namespace AnalysisOfChangeEngine.Common


[<AutoOpen>]
module rec Types =

    open System
    open System.Collections.Generic


    type PolicyID = string
    type Reason = string
    type CleansingChanges = CleansingChange list

    type ParseOutcome<'T> =
        Result<'T * CleansingChanges, PolicyID option * Reason>

    [<NoEquality; NoComparison>]
    type CleansingChange =
        {
            Field:  string
            From:   string
            To:     string
            Reason: string
        }

    [<NoEquality; NoComparison>]
    type RunContext =
        {
            RunDate: DateOnly
        }

    type IPolicyRecord =
        interface
            abstract member ID: string with get
        end

    type IPolicyRecordSource<'T when 'T :> IPolicyRecord> =
        interface
            abstract member IDs: string seq
            abstract member GetDataForIDs: string seq -> Map<string, Result<'T, string>>
        end

    type ILogger =
        interface
            abstract member LogInfo: message: string -> unit
            abstract member LogDebug: message: string -> unit
            abstract member LogWarning: message: string -> unit
            abstract member LogError: message: string -> unit
        end


    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type PolicyInclusionType =
        | Add
        | Remove

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type StepValidationIssue =
        | Warning of string
        | Error of string

    type DataChangeValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    [<RequireQualifiedAccess>]
    module DataChangeValidator =
        let Ignore : DataChangeValidator<_, _> =
            fun _ -> List.empty

    type NonDataChangeValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationIssue list

    [<RequireQualifiedAccess>]
    module NonDataChangeValidator =
        let Ignore : NonDataChangeValidator<_, _> =
            fun _ -> List.empty

    type IStepHeader =
        interface
            abstract member Title: string with get
            abstract member Description: string with get
        end

    [<NoEquality; NoComparison>]
    type OpeningStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            DataSource: IPolicyRecordSource<'TPolicyRecord>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description                

    [<NoEquality; NoComparison>]
    type RegressionStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Validator: NonDataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description    

    [<NoEquality; NoComparison>]
    type DataChangeStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            DataChanges: Map<PolicyID, 'TPolicyRecord>
            Validator: DataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    [<NoEquality; NoComparison>]
    type RemoveExitedStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            ToRemove: PolicyID Set
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    [<NoEquality; NoComparison>]
    type ParameterChangeStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Validator: NonDataChangeValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type InteriorStepType<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        | Regression of RegressionStep<'TPolicyRecord, 'TStepResults>
        | DataChange of DataChangeStep<'TPolicyRecord, 'TStepResults>
        | ParameterChange of ParameterChangeStep<'TPolicyRecord, 'TStepResults>

    [<NoEquality; NoComparison>]
    type InteriorStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Type: InteriorStepType<'TPolicyRecord, 'TStepResults>
        }        

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 

    [<NoEquality; NoComparison>]
    type AddNewBusinessStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            ToAdd: PolicyID Set
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 


    [<AbstractClass>]
    type AbstractWalk<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord>
        (logger: ILogger, productName: string) =
            let _interiorSteps =
                new List<InteriorStep<'TPolicyRecord, 'TStepResults>> ()

            abstract member opening: OpeningStep<'TPolicyRecord, 'TStepResults> with get

            abstract member openingRegression: RegressionStep<'TPolicyRecord, 'TStepResults> with get

            abstract member removeExited: RemoveExitedStep<'TPolicyRecord, 'TStepResults> with get

            // --- INTERIOR STEPS SUPPLIED BY USER ---
            member val interiorSteps =
                // This will reflect any live updates to the interior steps list above.
                _interiorSteps.AsReadOnly () 

            abstract member addNewBusiness: AddNewBusinessStep<'TPolicyRecord, 'TStepResults> with get

            member _.registerInteriorStep (step: InteriorStep<'TPolicyRecord, 'TStepResults>) =
                do logger.LogDebug (sprintf "Walk for '%s': Registering step '%s'." productName step.Title)
                do _interiorSteps.Add step
                // Pass through the supplied step.
                step

