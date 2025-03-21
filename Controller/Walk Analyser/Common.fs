﻿
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser

open System
open System.Reflection
open FSharp.Quotations
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.StateMonad


[<CustomEquality; CustomComparison>]
type SourceElementApiCallDependency<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        Requestor       : IApiRequestor<'TPolicyRecord>
        OutputProperty  : PropertyInfo
    }

    // Inspired by...
    // https://www.compositional-it.com/news-blog/custom-equality-and-comparison-in-f/

    // Although this seems like a lot of work, it allows us to identify
    // distinct dependencies. This allows them to be used as Map<_, _> keys
    // and within Set<_>.
    override this.Equals (other) =
        match other with
        | :? SourceElementApiCallDependency<'TPolicyRecord> as other' ->
                (this :> IEquatable<_>).Equals other'
            | _ ->
                false

    override this.GetHashCode () =
        HashCode.Combine (this.Requestor, this.OutputProperty)

    interface IEquatable<SourceElementApiCallDependency<'TPolicyRecord>> with
        member this.Equals (other) =
            this.Requestor = other.Requestor
                && this.OutputProperty = other.OutputProperty

    interface IComparable<SourceElementApiCallDependency<'TPolicyRecord>> with
        member this.CompareTo (other) =
            match this.Requestor.Name.CompareTo other.Requestor.Name with
                // Given these are for the same record type, not an unreasonable approach.
                | 0 -> this.OutputProperty.Name.CompareTo other.OutputProperty.Name
                | x -> x

    interface IComparable with
        member this.CompareTo (other) =
            match other with
            | :? SourceElementApiCallDependency<'TPolicyRecord> as other' ->
                (this :> IComparable<_>).CompareTo other'
            | _ ->
                failwith "Cannot compare different types of source element dependencies."


// Used to track dependencies between members of the current step results.
[<CustomEquality; CustomComparison>]
type SourceElementResultDependency<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        ElementProperty: PropertyInfo
    }

    override this.Equals (other) =
        match other with
        | :? SourceElementResultDependency<'TPolicyRecord> as other' ->
                (this :> IEquatable<_>).Equals other'
            | _ ->
                false

    override this.GetHashCode () =
        this.ElementProperty.GetHashCode ()

    interface IEquatable<SourceElementResultDependency<'TPolicyRecord>> with
        member this.Equals (other) =
            this.ElementProperty = other.ElementProperty

    interface IComparable<SourceElementResultDependency<'TPolicyRecord>> with
        member this.CompareTo (other) =
            this.ElementProperty.Name.CompareTo other.ElementProperty.Name

    interface IComparable with
        member this.CompareTo (other) =
            match other with
            | :? SourceElementResultDependency<'TPolicyRecord> as other' ->
                (this :> IComparable<_>).CompareTo other'
            | _ ->
                failwith "Cannot compare different types of source element dependencies."


// Used to track all of the dependencies for a given step element.
[<NoEquality; NoComparison>]
type SourceElementDependencies<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        ApiCalls                : Map<SourceElementApiCallDependency<'TPolicyRecord>, Var>
        CurrentResults          : Map<SourceElementResultDependency<'TPolicyRecord>, Var>
    }

module SourceElementDependencies =
    let empty =
        {
            ApiCalls = Map.empty
            CurrentResults = Map.empty
        }

    let addApiCall (requestor, outputProperty) =
        state {
            let! currentDependencies =
                State.get

            let dependency =
                {
                    Requestor       = requestor
                    OutputProperty  = outputProperty
                }

            let varDef, newDependencies =
                currentDependencies
                |> _.ApiCalls
                |> Map.tryFind dependency
                |> function
                    | Some varDef ->
                        varDef, currentDependencies

                    | None ->
                        let newVarName =
                            "Api Call <" + requestor.Name + ">.<" + outputProperty.Name + ">"

                        let newVarDef =
                            Var (newVarName, outputProperty.PropertyType)

                        let newDependencies =
                            {
                                currentDependencies with
                                    ApiCalls =
                                        currentDependencies.ApiCalls.Add (dependency, newVarDef)
                            }

                        newVarDef, newDependencies

            // Potentially we're just re-adding the same set of dependencies.
            do! State.put newDependencies

            return varDef
        }

    let addCurrentResult elementProperty =
        state {
            let! currentDependencies =
                State.get

            let dependency =
                {
                    ElementProperty = elementProperty
                }

            let varDef, newDependencies =
                currentDependencies
                |> _.CurrentResults
                |> Map.tryFind dependency
                |> function
                    | Some varDef ->
                        varDef, currentDependencies

                    | None ->
                        let newVarName =
                            "Current Result <" + elementProperty.Name + ">"

                        let newVarDef =
                            Var (newVarName, elementProperty.PropertyType)

                        let newDependencies =
                            {
                                currentDependencies with
                                    CurrentResults =
                                        currentDependencies.CurrentResults.Add (dependency, newVarDef)
                            }

                        newVarDef, newDependencies

            do! State.put newDependencies

            return varDef
        }
          

[<NoEquality; NoComparison>]
type SourceElementDefinition<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        Dependencies        : SourceElementDependencies<'TPolicyRecord>
        //Original            : Expr
        Rebuilt             : Expr
    }

[<NoEquality; NoComparison>]
type ParsedSource<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        ElementDefinitions  : Map<string, SourceElementDefinition<'TPolicyRecord>>
        // Collection of API calls across for the entire step (ie. across all elements).
        ApiCalls            : SourceElementApiCallDependency<'TPolicyRecord> Set
        Ordering            : string list
    }

[<NoEquality; NoComparison>]
type OpeningDataStage<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {        
        OpeningStepHeader           : IStepHeader
        // This does NOT include the data change step header above.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        // These are the API calls arising from steps within this data stage.
        WithinStageApiCalls         : SourceElementApiCallDependency<'TPolicyRecord> Set
    }

[<NoEquality; NoComparison>]
type PostOpeningDataStage<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        // Note that not all data changes occur at a DataChangeStep!
        // Data changes can also ocurr at the penultimate step.
        DataChangeStep              : IDataChangeStep<'TPolicyRecord>
        // Although a data change step has no source, it inherits that
        // as used for the previous step.
        DataChangeStepParsedSource  : ParsedSource<'TPolicyRecord>
        // This does NOT include the data change step header above.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        // These are the API calls arising from steps within this data stage.
        // This will include those arising from the data change step itself
        // which will have inherited the source from the previous step.
        WithinStageApiCalls         : SourceElementApiCallDependency<'TPolicyRecord> Set
    }

[<NoEquality; NoComparison>]
type ParsedWalk<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        PolicyRecordVarDef                      : Var
        // For exiting records, they only fall within the opening data stage.
        //ExitedRecordDataStage                   : OpeningDataStage<'TPolicyRecord>
        // For records that have remained in-force over the period, they have both
        // an opening data stage (which could include additional steps relative to
        // an exited record) as well as all data stages afterwards, which would
        // include the move to closing data as a minimum.
        RemainingRecordOpeningDataStage         : OpeningDataStage<'TPolicyRecord>
        RemainingRecordPostOpeningDataStages    : PostOpeningDataStage<'TPolicyRecord> list
        // New records just need to know the source used for the prior step.
        //NewRecordSource                         : ParsedSource<'TPolicyRecord>
    }
