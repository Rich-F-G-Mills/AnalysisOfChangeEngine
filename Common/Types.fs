
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

    type NonDataChangeValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationIssue list

    type OpeningStepValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list

    type AddNewRecordsValidator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        'TPolicyRecord * 'TStepResults -> StepValidationIssue list

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
            Validator: OpeningStepValidator<'TPolicyRecord, 'TStepResults>
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
    type RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
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
    type InteriorStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        | Regression of Step: RegressionStep<'TPolicyRecord, 'TStepResults>
        | DataChange of Step: DataChangeStep<'TPolicyRecord, 'TStepResults>
        | ParameterChange of Step: ParameterChangeStep<'TPolicyRecord, 'TStepResults>     

        interface IStepHeader with
            member this.Title =
                match this with
                | Regression { Title = title }
                | DataChange { Title = title }
                | ParameterChange { Title = title } ->
                    title

            member this.Description =
                match this with
                | Regression { Description = description }
                | DataChange { Description = description }
                | ParameterChange { Description = description } ->
                    description

    [<NoEquality; NoComparison>]
    type AddNewRecordsStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            Title: string
            Description: string
            Validator: AddNewRecordsValidator<'TPolicyRecord, 'TStepResults>
        }

        interface IStepHeader with
            member this.Title = this.Title
            member this.Description = this.Description 


    [<AbstractClass>]
    type AbstractWalk<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> (
        logger: ILogger,
        productName: string,
        openingDataSource: IPolicyRecordSource<'TPolicyRecord>,
        closingDataSource: IPolicyRecordSource<'TPolicyRecord>
    ) =
        let _interiorSteps =
            new List<InteriorStep<'TPolicyRecord, 'TStepResults>> ()

        abstract member opening: OpeningStep<'TPolicyRecord, 'TStepResults> with get

        abstract member openingRegression: RegressionStep<'TPolicyRecord, 'TStepResults> with get

        abstract member removeExitedRecords: RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> with get

        // --- INTERIOR STEPS SUPPLIED BY USER ---
        member val interiorSteps =
            // This will reflect any live updates to the interior steps list above.
            _interiorSteps.AsReadOnly () 

        abstract member addNewRecords: AddNewRecordsStep<'TPolicyRecord, 'TStepResults> with get

        member _.registerInteriorStep (step: InteriorStep<'TPolicyRecord, 'TStepResults>) =
            let stepHeader =
                step :> IStepHeader

            do logger.LogDebug (sprintf "Walk for '%s': Registering step '%s'." productName stepHeader.Title)
            do _interiorSteps.Add step
            // Pass through the supplied step.
            step
