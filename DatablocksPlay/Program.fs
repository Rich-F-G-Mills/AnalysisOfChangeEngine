
module internal Program =

    type NonEmptyList<'T> =
        | SingleItem of 'T
        | MultipleItems of 'T * NonEmptyList<'T>

    let inline (||.) x y = 
        MultipleItems (x, y)

    [<RequireQualifiedAccess>]
    module NonEmptyList =

        let inline singleton x =
            SingleItem x


    type NonEmptyListBuilder () =
        member _.Yield (x) = SingleItem x
        member _.Delay (x) = x()
        member _.Combine (x, y) =
            match x with
            | SingleItem x' ->
                MultipleItems (x', y)
            | _ ->
                failwith "Unexpected error."

    let nonEmptyList =
        new NonEmptyListBuilder ()


    [<EntryPoint>]
    let main _ =
        
        let myList: NonEmptyList<string> =
            nonEmptyList {
                yield "x"
            } 

        0
    