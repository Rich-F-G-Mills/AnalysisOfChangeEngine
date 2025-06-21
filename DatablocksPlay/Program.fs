
open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow


module internal Program =

    type CalcRequest =
        {
            WaitTime    : int
            Message     : string
        }


    let taskQueue =
        new BufferBlock<CalcRequest> ()

    let actionBlockOptions =
        new ExecutionDataflowBlockOptions(
            MaxDegreeOfParallelism = 1,
            BoundedCapacity = 1,
            SingleProducerConstrained = true
        )

    let actionDelegate idx (req: CalcRequest) =
        do printfn "Beginning request '%s' on processor #%i." req.Message idx

        do Thread.Sleep req.WaitTime

        do printfn "Finishing request '%s' on processor #%i." req.Message idx

    let processor1 =
        new ActionBlock<CalcRequest> (actionDelegate 0, actionBlockOptions)

    let processor2 =
        new ActionBlock<CalcRequest> (actionDelegate 1, actionBlockOptions)

    let linkOptions =
        new DataflowLinkOptions(
            PropagateCompletion = true
        )


    [<EntryPoint>]
    let main _ = 
        use _ =
            taskQueue.LinkTo (processor1, linkOptions)

        use _ =
            taskQueue.LinkTo (processor2, linkOptions)

        let requests =
            Array.init 10 (fun idx ->
                {
                    WaitTime = 1000
                    Message = $"Request #{idx}"
                })

        for r in requests do
            do ignore <| taskQueue.Post r

        do taskQueue.Completion.ContinueWith(fun t -> do printfn "Queue Completed").
        do processor1.Completion.ContinueWith(fun t -> do printfn "P1 Completed").Start()
        do processor2.Completion.ContinueWith(fun t -> do printfn "P2 Completed").Start()

        do taskQueue.Complete ()

        do Thread.Sleep 5000

        0