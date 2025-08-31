
module internal Program =

    open FSharp.Quotations


    type IVariable<'T> =
        interface end


    [<Sealed>]
    type TimeIndependentVariable<'TParams, 'TProduct, 'T> (x: Expr<'TParams -> 'TProduct -> 'T>) =
        interface IVariable<'T>

        member val Definition =
            x


    [<Sealed>]
    type AccrualVariable<'TParams, 'TProduct, 'T> (x: Expr<'TParams -> 'TProduct -> AccrualVariable<'TParams, 'TProduct, 'T> -> 'T>) =
        interface IVariable<'T>

        member _.PriorValue: AccrualVariable<'TParams, 'TProduct, 'T> =
            failwith "Unreachable code."

        member val Definition =
            x


    let inline v (_: IVariable<'T>) =
        Unchecked.defaultof<'T>


    [<AbstractClass>]
    type BaseProduct<'TParams, 'TProduct> () =
        member _.timeIndependentVariable x =
            new TimeIndependentVariable<'TParams, 'TProduct, _> (x)

        member _.accrualVariable x =
            new AccrualVariable<'TParams, 'TProduct, _> (x)

    type ProdParams =
        {
            IntRate: float
        }


    type MyProduct () as P =
        inherit BaseProduct<ProdParams, MyProduct> ()

        member val paramI =
            P.timeIndependentVariable <@
                fun pars _ -> 0.5 * pars.IntRate
            @>

        member val paramS =
            P.accrualVariable <@
                fun pars thisProd thisVar -> 
                    v(thisVar.PriorValue) + v(thisProd.paramI) + v(thisProd.paramT) * pars.IntRate
            @>

        member val paramT =
            P.timeIndependentVariable <@
                fun pars thisProd -> v(thisProd.paramI) * 2.0 + pars.IntRate
            @>


    [<EntryPoint>]
    let main _ =
        
        let myProduct =
            new MyProduct ()

        do printfn "%A" myProduct.paramS.Definition

        let vMI =
            match <@ v @> with
            | Patterns.Lambda(_, Patterns.Call (_, mi, _)) ->
                mi.GetGenericMethodDefinition ()
            | _ ->
                failwith "Unexpected pattern."

        do printfn "%A" vMI

        0
    