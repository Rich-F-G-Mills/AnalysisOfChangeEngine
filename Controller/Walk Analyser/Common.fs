
// This contains types used throughout the walk analysis logic.

namespace AnalysisOfChangeEngine.Controller.WalkAnalyser

open System
open System.Reflection
open FSharp.Quotations
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Common.StateMonad


[<CustomEquality; CustomComparison>]
type SourceElementApiCallDependency<'TPolicyRecord> =
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


// Used to track all of the dependencies for a given step element.
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
                    Requestor        = endpoint
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
        // Only intended to be used for testing purposes. The first and
        // second object arrays correspond to the API and current result
        // tuples respectively.
        ApiCallsTupleType       : Type
        CurrentResultsTupleType : Type
        WrappedInvoker          : ('TPolicyRecord * obj array * obj array) -> obj
    }

[<NoEquality; NoComparison>]
type ParsedSource<'TPolicyRecord> =
    {
        ElementDefinitions  : Map<string, SourceElementDefinition<'TPolicyRecord>>
        ApiCallsTupleType   : Type
        ApiCalls            : SourceElementApiCallDependency<'TPolicyRecord> Set
        RebuiltSourceExpr   : Expr
        WrappedInvoker      : ('TPolicyRecord * obj array) -> obj
    }

[<NoEquality; NoComparison>]
type OpeningDataStage<'TPolicyRecord> =
    {        
        // This does include the exited records step.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        // These are the API calls arising from the within stage steps above.
        WithinStageApiCalls         : SourceElementApiCallDependency<'TPolicyRecord> Set
    }

[<NoEquality; NoComparison>]
type PostOpeningDataStage<'TPolicyRecord> =
    {
        // Note that NOT all data changes occur at a DataChangeStep!
        // Data changes can also ocurr at the penultimate step.
        DataChangeStep              : IDataChangeStep<'TPolicyRecord>
        // Although a data change step has no source, it inherits that
        // as used for the previous step.
        DataChangeStepParsedSource  : ParsedSource<'TPolicyRecord>
        // This does NOT include the data change step header above.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        // These are the API calls arising from steps within this data stage.
        // This will INCLUDE those arising from the data change step itself
        // which will have inherited the source from the previous step.
        WithinStageApiCalls         : SourceElementApiCallDependency<'TPolicyRecord> Set
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
type ParsedWalk<'TPolicyRecord> =
    {
        ParsedSteps             : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        OpeningDataStage        : OpeningDataStage<'TPolicyRecord>
        PostOpeningDataStages   : PostOpeningDataStage<'TPolicyRecord> list
    }
