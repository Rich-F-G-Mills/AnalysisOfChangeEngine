
// This contains types used throughout the walk analysis logic.

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
        ApiCalls                : Set<SourceElementApiCallDependency<'TPolicyRecord>>
        CurrentResults          : Set<string>
    }

module internal SourceElementDependencies =
    let empty =
        {
            ApiCalls = Set.empty
            CurrentResults = Set.empty
        }

    let registerApiCall (requestor, outputProperty) =
        stateful {
            let! (apiCallVarDefs, elementDependencies) =
                Stateful.get

            let dependency =
                {
                    Requestor       = requestor
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
                        "Api Call <" + requestor.Name + ">.<" + outputProperty.Name + ">"

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

                | Some varDef, true ->
                    apiCallVarDefs, elementDependencies, varDef

                | Some varDef, false ->
                    let newElementDependencies' = {
                        elementDependencies with
                            ApiCalls =
                                elementDependencies.ApiCalls
                                |> Set.add dependency
                        }

                    apiCallVarDefs, newElementDependencies', varDef

            // Potentially we're just re-adding the same set of dependencies.
            do! Stateful.put (newApiCallVarDefs, newElementDependencies)

            return varDef
        }

    let registerCurrentResult elementName =
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
type SourceElementDefinition<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        Dependencies            : SourceElementDependencies<'TPolicyRecord>
        OriginalExprBody        : Expr
        // Wrapped as part of a lambda.
        RebuiltExprBody         : Expr
        // Only intended to be used for testing purposes. The first and
        // second object arrays correspond to the API and current result
        // tuples respectively.
        ApiCallsTupleType       : Type
        CurrentResultsTupleType : Type
        WrappedInvoker          : ('TPolicyRecord * obj array * obj array) -> obj
    }

[<NoEquality; NoComparison>]
type ParsedSource<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        ElementDefinitions  : Map<string, SourceElementDefinition<'TPolicyRecord>>
        // Collection of API calls across for the entire step (ie. across ALL source elements).
        ApiCallsTupleType   : Type
        ApiCalls            : SourceElementApiCallDependency<'TPolicyRecord> Set
        RebuiltSourceExpr   : Expr
        // The obj array corresponds to all API call results for the step.
        WrappedInvoker      : ('TPolicyRecord * obj array) -> obj
    }

[<NoEquality; NoComparison>]
type OpeningDataStage<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {        
        OpeningStepHeader           : IStepHeader
        // This does NOT include the data change step header above NOR
        // the remove exited records step itself.
        WithinStageSteps            : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        // These are the API calls arising from steps within this data stage.
        // This does not reflect the opening step itself which has no source (nor can it).
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
        PostOpeningParsedSteps                  : (IStepHeader * ParsedSource<'TPolicyRecord>) list
        // For exiting records, they only fall within the opening data stage.
        //ExitedRecordDataStage                   : OpeningDataStage<'TPolicyRecord>
        // For records that have remained in-force over the period, they have both
        // an opening data stage (which could include additional steps relative to
        // an exited record) as well as all data stages thereafter, which would
        // include the move to closing data as a minimum.
        RemainingRecordOpeningDataStage         : OpeningDataStage<'TPolicyRecord>
        RemainingRecordPostOpeningDataStages    : PostOpeningDataStage<'TPolicyRecord> list
        // New records just need to know the source used for the prior step.
        //NewRecordSource                         : ParsedSource<'TPolicyRecord>
    }
