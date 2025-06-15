
namespace AnalysisOfChangeEngine.ApiProvider.Excel


module internal Attributes =

    open System
    open FSharp.Reflection


    [<AbstractClass>]
    type MapFromAttributeBase<'T when 'T:comparison> (source: 'T) =
        inherit Attribute ()

        member val Source = source

    [<Sealed>]
    type MapFromIntAttribute (source: _) =
        inherit MapFromAttributeBase<int> (source)


    let getMappingsForType<'TSource, 'TTarget when 'TSource:comparison and 'TTarget:comparison> =
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
            failwith (sprintf "Not all sources of type '%s' are distinct for target of type '%s'." typeof<'TSource>.Name typeof<'TTarget>.Name)
        elif anyDuplicated targets then
            failwith (sprintf "Not all targets of type '%s' are distinct." typeof<'TTarget>.Name)
        else
            mappings

    let private makeStrict<'T1, 'T2 when 'T1:comparison> (mapper: 'T1 -> 'T2 option) from =
        match mapper from with
        | Some ``to`` -> ``to``
        | None -> failwith (sprintf "Unable to map '%A' to target type '%s'." from typeof<'T2>.Name)

    let createMapperFromNativeToType<'TSource, 'TTarget when 'TSource:comparison and 'TTarget:comparison> =
        // do printfn "Creating mapper from native '%s' to '%s'." typeof<'TSource>.Name typeof<'TTarget>.Name
        
        let cases =
            getMappingsForType<'TSource, 'TTarget>
            |> Map.ofArray

        fun source ->
            Map.tryFind source cases

    let createStrictMapperFromNativeToType<'TSource, 'TTarget when 'TSource:comparison and 'TTarget:comparison> =
        makeStrict createMapperFromNativeToType<'TSource, 'TTarget>
