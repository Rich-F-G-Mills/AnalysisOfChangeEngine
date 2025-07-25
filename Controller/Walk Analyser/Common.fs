﻿
// This contains types used throughout the walk analysis logic.

namespace AnalysisOfChangeEngine.Controller.WalkAnalyser

open System
open System.Reflection
open FSharp.Quotations
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Common.StateMonad


[<AutoOpen>]
module internal Helpers =
    let internal checkStepType<'TStep when 'TStep :> IStepHeader> (hdr: IStepHeader) =
        match hdr with
        | :? 'TStep -> true
        | _ -> false


/// Represents a request for a specific output from a specific API requesto.
[<CustomEquality; CustomComparison>]
type SourceElementApiCallDependency<'TPolicyRecord> =
    {
        Requestor       : AbstractApiRequestor<'TPolicyRecord>
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
            this.Requestor.Name = other.Requestor.Name
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


/// For a specific element within a step, tracks all API dependencies and dependencies
/// within the step itself.
[<NoEquality; NoComparison>]
type SourceElementDependencies<'TPolicyRecord> =
    {
        ApiCalls                : Set<SourceElementApiCallDependency<'TPolicyRecord>>
        CurrentResults          : Set<string>
    }

module internal SourceElementDependencies =
    let internal empty =
        {
            ApiCalls        = Set.empty
            CurrentResults  = Set.empty
        }

    let internal registerApiCall (endpoint, outputProperty) =
        stateful {
            let! (apiCallVarDefs, elementDependencies) =
                Stateful.get

            let dependency =
                {
                    Requestor       = endpoint
                    OutputProperty  = outputProperty
                }

            let foundVarDef =
                apiCallVarDefs
                |> Map.tryFind dependency

            let isElementDependency =
                elementDependencies.ApiCalls
                |> Set.contains dependency

            let newApiCallVarDefs, newElementDependencies, varDef =
                match foundVarDef, isElementDependency with
                | None, true ->
                    failwith "Unexpected error; inconsistent dependency."
            
                | None, false ->
                    let newVarName =
                        "Api Call <" + endpoint.Name + ">.<" + outputProperty.Name + ">"

                    let newVarDef =
                        Var (newVarName, outputProperty.PropertyType)

                    let newElementDependencies' = {
                        elementDependencies with
                            ApiCalls =
                                elementDependencies.ApiCalls
                                |> Set.add dependency
                        }

                    let newApiCallVarDefs' =
                        apiCallVarDefs
                        |> Map.add dependency newVarDef 

                    newApiCallVarDefs', newElementDependencies', newVarDef

                | Some varDef', true ->
                    apiCallVarDefs, elementDependencies, varDef'

                | Some varDef', false ->
                    let newElementDependencies' = {
                        elementDependencies with
                            ApiCalls =
                                elementDependencies.ApiCalls
                                |> Set.add dependency
                        }

                    apiCallVarDefs, newElementDependencies', varDef'

            // Potentially we're just re-adding the same set of dependencies.
            do! Stateful.put (newApiCallVarDefs, newElementDependencies)

            return varDef
        }

    let internal registerCurrentResult elementName =
        stateful {
            let! (apiCallVarDefs, elementDependencies) =
                Stateful.get

            let newElementDependencies =
                {
                    elementDependencies with
                        CurrentResults =
                            elementDependencies.CurrentResults
                            |> Set.add elementName
                }

            do! Stateful.put (apiCallVarDefs, newElementDependencies)

            return ()
        }
          

[<NoEquality; NoComparison>]
type SourceElementDefinition<'TPolicyRecord> =
    {
        Dependencies            : SourceElementDependencies<'TPolicyRecord>
        OriginalExprBody        : Expr
        RebuiltExprBody         : Expr
    }

[<NoEquality; NoComparison>]
type ParsedSource<'TPolicyRecord, 'TStepResults> =
    {
        ElementDefinitions  : Map<string, SourceElementDefinition<'TPolicyRecord>>
        ApiCalls            : SourceElementApiCallDependency<'TPolicyRecord> Set
        RebuiltSourceExpr   : Expr
        /// The API outputs, passed in via the obj array, will be in the SAME order
        /// as the ApiCalls property above.
        Invoker             : ('TPolicyRecord * obj array) -> 'TStepResults
    }

[<NoEquality; NoComparison>]
type OpeningDataStage<'TPolicyRecord, 'TStepResults> =
    {        
        // This does include the exited records step.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord, 'TStepResults>) list
    }

[<NoEquality; NoComparison>]
type PostOpeningDataStage<'TPolicyRecord, 'TStepResults> =
    {
        // Note that NOT all data changes occur at a DataChangeStep!
        // Data changes can also ocurr at the penultimate step.
        DataChangeStep              : IDataChangeStep<'TPolicyRecord>
        // This DOES include the data change step header above.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord, 'TStepResults>) list
    }

(*
Design decision:
    So what about exited records or new records? Why are we only concerning ourselves
    with those remaining in-force?

    For records which are exiting, the only steps of interest will be:
        * The opening re-run (ie. regression) step.

    For new records, we only care about:
        * The new records step; which is just the final element of the parsed steps tuple below.

    In both cases, we extract the relevant "bit" from the members below.
*)

[<NoEquality; NoComparison>]
type ParsedWalk<'TPolicyRecord, 'TStepResults> =
    {
        ParsedSteps             : (IStepHeader * ParsedSource<'TPolicyRecord, 'TStepResults>) list
        OpeningDataStage        : OpeningDataStage<'TPolicyRecord, 'TStepResults>
        PostOpeningDataStages   : PostOpeningDataStage<'TPolicyRecord, 'TStepResults> list
    }
