
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
                            "Api Call [" + requestor.Name + "].[" + outputProperty.Name + "]"

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
                            "Current Result [" + elementProperty.Name + "]"

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
        

        
       

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type ProcessedSourceElement<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
    {
        Dependencies    : SourceElementDependencies<'TPolicyRecord>
        OriginalBody    : Expr
        RebuiltBody     : Expr
    }

