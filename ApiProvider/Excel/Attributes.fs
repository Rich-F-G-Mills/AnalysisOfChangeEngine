
namespace AnalysisOfChangeEngine.ApiProvider.Excel


[<AutoOpen>]
module Attributes =

    open System
    open System.Reflection
    open FSharp.Reflection
    open FsToolkit.ErrorHandling


    [<AbstractClass>]
    [<AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)>]
    type internal MapFromAttributeBase<'T when 'T:comparison> (source: 'T) =
        inherit Attribute ()

        member val Source =
            source with get

    [<Sealed>]
    type internal MapFromIntAttribute (source: _) =
        inherit MapFromAttributeBase<int> (source)


    [<Sealed>]
    [<AttributeUsage(AttributeTargets.Property, AllowMultiple = false)>]
    type ExcelRangeAliasAttribute (rangeName: string) =
        inherit Attribute ()

        member val RangeName =
            rangeName with get


    let internal getRangeNameFromPI (pi: PropertyInfo) =        
        pi.GetCustomAttribute<ExcelRangeAliasAttribute>()
        |> Option.ofNull
        |> function
            | None -> pi.Name
            | Some attr -> attr.RangeName


    let internal getMappingsForType<'TSource, 'TTarget when 'TSource:comparison and 'TTarget:comparison> =
        let anyDuplicated arr =
            let distinct =
                Array.distinct arr

            distinct.Length < arr.Length

        let mappings =
            FSharpType.GetUnionCases typeof<'TTarget>
            |> Array.choose (fun c ->
                c.GetCustomAttributes typeof<MapFromAttributeBase<'TSource>>
                |> Array.map (fun a -> a :?> MapFromAttributeBase<'TSource>)
                |> Array.tryExactlyOne
                |> Option.map (fun a -> a.Source, FSharpValue.MakeUnion (c, Array.empty) :?> 'TTarget))

        let sources =
            mappings |> Array.map fst

        let targets =
            mappings |> Array.map snd

        if anyDuplicated sources then
            failwithf "Not all sources of type '%s' are distinct for target of type '%s'." typeof<'TSource>.Name typeof<'TTarget>.Name
        elif anyDuplicated targets then
            failwithf "Not all targets of type '%s' are distinct." typeof<'TTarget>.Name
        else
            mappings

    let private makeStrict<'T1, 'T2 when 'T1:comparison> (mapper: 'T1 -> 'T2 option) from =
        match mapper from with
        | Some ``to`` -> ``to``
        | None -> failwithf "Unable to map '%A' to target type '%s'." from typeof<'T2>.Name

    let internal createMapperFromNativeToType<'TSource, 'TTarget when 'TSource:comparison and 'TTarget:comparison> =
        // do printfn "Creating mapper from native '%s' to '%s'." typeof<'TSource>.Name typeof<'TTarget>.Name
        
        let cases =
            getMappingsForType<'TSource, 'TTarget>
            |> Map.ofArray

        fun source ->
            Map.tryFind source cases

    let internal createStrictMapperFromNativeToType<'TSource, 'TTarget when 'TSource:comparison and 'TTarget:comparison> =
        makeStrict createMapperFromNativeToType<'TSource, 'TTarget>
