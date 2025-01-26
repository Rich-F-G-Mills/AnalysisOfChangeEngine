    
namespace AnalysisOfChangeEngine.ProviderImplementations

open System.Reflection
open FSharp.Core.CompilerServices
open FSharp.Quotations
open FSharp.Reflection
open FsToolkit.ErrorHandling
open ProviderImplementation.ProvidedTypes


[<RequireQualifiedAccess>]
module private DocStrings =

    // TODO - Why isn't this working?
    let [<Literal>] SplitRowType =
        """<summary>
            <para>
                Type provider that generates a simple record(-like) type that can provide <c>string</c> representations
                of underlying row content.
            </para>
            <example>
                For example:
                <code>
                    type SplitRow = SplitRowType&lt;"Pol_ID-&gt;POLICY_ID,DOE-&gt;ENTRY_DATE,ENTRY_AGE"&gt;
                </code>
            </example>
        </summary>
        <param name="RequiredColumns">A string containing a comma-separated list of columns expected
        to be found. An optional mapping utilizing <c>-&gt;</c> can also be used.</param>"""

    let generateForProperty (mapping: ColumnMapping) =
        sprintf "<summary>String content taken from underlying '<c>%s</c>' column.</summary>" mapping.WithinFile

    let generateForSplitterFactory (typeName: string) =
        """When provided with the available columns from the underlying source, and assuming required
           columns are available, will return a lambda that will convert an array of row contents
           into the owning (record-like) type."""


[<TypeProvider>]
type public SplitRowTypeProvider (config: TypeProviderConfig) as this =
    inherit TypeProviderForNamespaces (config)

    static let rootNS =
        "AnalysisOfChangeEngine.TypeProviders"

    static let assembly =
        Assembly.GetExecutingAssembly ()

    let providerType =
        new ProvidedTypeDefinition (assembly, rootNS, "SplitRowType", None, hideObjectMethods = true, nonNullable = true)

    do providerType.AddXmlDoc DocStrings.SplitRowType

    let staticParams =
        [ new ProvidedStaticParameter("RequiredColumns", typeof<string>) ]

    let instantiator (typeName: string) (paramValues: obj array) =
        let specStr =
            match paramValues with
            | [| :? string as cols |] ->
                cols
            | _ ->
                failwith "Unexpected static parameter."

        // We WANT to raise an exception if the specification string isn't properly formatted.
        let mappings =
            ColumnMapping.parseSpecification specStr
            |> Result.defaultWith failwith

        // Represents the underlying tuple that will contain the column contents.
        let splitRowErasedType =
            let types =
                Array.init mappings.Length (fun _ -> typeof<string>)

            FSharpType.MakeTupleType types

        // This is the type which will contain properties for each of our mapped columns.
        let providedSplitRowType =
            ProvidedTypeDefinition(
                assembly,
                rootNS,
                typeName,
                Some splitRowErasedType,
                hideObjectMethods = true,
                nonNullable = true
            )

        // Add in a property and corresponding getter for each mapped column.
        for idx, mapping in List.indexed mappings do
            let getterCode (Singleton (this: Expr)) =
                Expr.TupleGet (this, idx)

            let property =
                new ProvidedProperty (mapping.MemberName, typeof<string>, getterCode)

            do property.AddXmlDoc (DocStrings.generateForProperty mapping)

            do providedSplitRowType.AddMember property

        let inline makeUnaryFunctionType (inputType, outputType) =
            ProvidedTypeBuilder.MakeGenericType(
                typedefof<_ -> _>,
                [ inputType; outputType ]
            )

        let inline makeResultType (okType, errorType) =
            ProvidedTypeBuilder.MakeGenericType(
                typedefof<Result<_, _>>,
                [ okType; errorType ]
            )

        // The return type of our static method is...
        //      Result<string array -> Result<..., string>, string>
        let rowSplitterFactoryReturnType =
            makeResultType(
                makeUnaryFunctionType(
                    typeof<string array>,
                    makeResultType(
                        providedSplitRowType,
                        typeof<string>
                    )
                ),
                typeof<string>
            )

        let createSplitterBody (Singleton availableHeaders) =
            let genericFactoryMI =
                typeof<RowSplitter>.GetMethod(nameof(RowSplitter.CreateSplitterBody))

            let typedFactoryMI =
                ProvidedTypeBuilder.MakeGenericMethod(genericFactoryMI, [ splitRowErasedType ])

            // Rather than provide a load of quoted code for the type provider machinery to
            // compile, seemed easier to create a call expression to an existing generic method above.
            // Note that even though we want to return an object of type 'providedSplitRowType',
            // we can return the underlying tuple instead without issue.
            Expr.Call (typedFactoryMI, [Expr.Value (specStr); availableHeaders])

        let createSplitterMethod =
            ProvidedMethod(
                "CreateSplitter",
                [ new ProvidedParameter("availableHeaders", typeof<string seq>) ],
                rowSplitterFactoryReturnType,
                invokeCode = createSplitterBody,
                isStatic = true
            )

        do createSplitterMethod.AddXmlDoc (DocStrings.generateForSplitterFactory typeName)

        do providedSplitRowType.AddMember createSplitterMethod

        providedSplitRowType

    do providerType.DefineStaticParameters (staticParams, instantiator)

    do this.AddNamespace (rootNS, [providerType])