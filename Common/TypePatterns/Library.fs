
namespace AnalysisOfChangeEngine.Common


[<AutoOpen>]
module TypePatterns =

    open System
    open System.Reflection
    open FSharp.Reflection
    open FsToolkit.ErrorHandling


    let (|NonOptionalUnion|_|) (t: Type) =
        option {
            do! Option.requireTrue
                 (FSharpType.IsUnion
                    // Not having this is leading to runtime failures in some cases.
                    // TODO - Bottom out exactly what combination of flags we should be using!
                    (t, BindingFlags.Public ||| BindingFlags.NonPublic))

            let recordColumns =
                FSharpType.GetUnionCases
                    (t, BindingFlags.Public ||| BindingFlags.NonPublic)

            return recordColumns
        }

    let (|NonOptionalNonParameterizedUnion|_|) (t: Type) =
        match t with
        | NonOptionalUnion unionCases ->
            option {
                let allNonParameterized =
                    unionCases
                    |> Array.forall (fun uc -> uc.GetFields().Length = 0)

                do! Option.requireTrue allNonParameterized

                return unionCases
            }

        | _ ->
            None

    let (|NonOptionalNonUnion|_|) (nonOptionalType: Type) =
        Option.requireFalse
            (FSharpType.IsUnion
                (nonOptionalType, BindingFlags.Public ||| BindingFlags.NonPublic))

    let (|Optional|_|) (maybeOptionalType: Type) =
        option {
            do! Option.requireTrue maybeOptionalType.IsGenericType

            let genericTypeDef =
                maybeOptionalType.GetGenericTypeDefinition()

            do! Option.requireTrue (genericTypeDef = typedefof<_ option>)

            let innerType =
                maybeOptionalType.GenericTypeArguments[0]

            return innerType
        }