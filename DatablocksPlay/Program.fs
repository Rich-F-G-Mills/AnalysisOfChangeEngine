
open System
open System.Reactive.Concurrency
open System.Reactive.Disposables
open System.Reactive.Subjects
open System.Threading
open System.Threading.Tasks.Dataflow
open System.Reactive.Linq


module internal Program =

    [<EntryPoint>]
    let main _ =
        use cts =
            new CancellationTokenSource ()

        let batchBlock =
            new BatchBlock<int> (
                3, 
                new GroupingDataflowBlockOptions (
                    CancellationToken = cts.Token
                )
            )

        let actionBlock =
            new ActionBlock<int []> (
                fun items ->
                    do printfn "Processing batch of %i items on thread %d"
                        items.Length
                        Environment.CurrentManagedThreadId
            )

        let linkOptions =
            new DataflowLinkOptions (
                PropagateCompletion = true
            )

        let _ =
            batchBlock.LinkTo (actionBlock, linkOptions)

        do ignore <| batchBlock.Post (1)
        do ignore <| batchBlock.Post (2)

        //do cts.Cancel ()
        do batchBlock.Complete ()

        do Thread.Sleep (1000)

        0
    