
open System
open System.Reactive
open FSharp.Control.Reactive
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow


module internal Program =

    [<NoEquality; NoComparison>]
    type RawPolicyRecord =
        {
            PolicyId    : string
            EntryAge    : int        
        }

    [<NoEquality; NoComparison>]
    type PolicyRecord =
        PolicyRecord of RawPolicyRecord

    [<RequireQualifiedAccess>]
    module PolicyRecord =
        let validate = function
            | { EntryAge = age } when age % 10 = 0 ->
                Error "Invalid entry age"
            | raw ->
                Ok (PolicyRecord raw)


    let loggingSubject =
        new Subjects.Subject<string> ()

    let logStr =
        loggingSubject.OnNext

    let outstandingPolicyIdsBuffer =
        new BufferBlock<string> (
            new DataflowBlockOptions (
                BoundedCapacity = 100 ,
                EnsureOrdered = true
            )
        )

    let policyIdsBatcher =
        new BatchBlock<_> (
            batchSize = 25,
            dataflowBlockOptions =
                new GroupingDataflowBlockOptions (
                    BoundedCapacity = 100,
                    Greedy = true
                )
        )

    let databaseReaderDelegate (recordIds: string array) =
        do logStr "Reading policy batch from database."

        recordIds
        |> Seq.indexed
        |> Seq.map (fun (idx, recId) ->
            PolicyRecord.validate { PolicyId = recId; EntryAge = idx })

    let databaseReaderBlock =
        new TransformManyBlock<_, _> (
            databaseReaderDelegate,
            new ExecutionDataflowBlockOptions (
                BoundedCapacity = 2,
                SingleProducerConstrained = true
            )
        )

    let parsedRecordProcessorBlock =
        let parsedRecordProcessorDelegate : _ -> Task = function
            | Ok (PolicyRecord polId) ->
                upcast backgroundTask {
                    do logStr $"Processing record '{polId}'."

                    do! Task.Delay (TimeSpan.FromMilliseconds 50)
                }

            | _ ->
                failwith "Unexpected error."

        new ActionBlock<Result<PolicyRecord, string>> (
            parsedRecordProcessorDelegate,
            new ExecutionDataflowBlockOptions(
                MaxDegreeOfParallelism = 5,
                SingleProducerConstrained = true,
                BoundedCapacity = 100
            )
        )

    let failedRecordParseSinkBlock =
        let failedRecordSinkDelegate = function
            | Error msg ->
                do logStr $"PARSE ERROR: {msg}"

            | _ ->
                failwith "Unexpected error."

        new ActionBlock<_> (
            failedRecordSinkDelegate
        )

    let linkOptions =
        new DataflowLinkOptions (
            PropagateCompletion = true
        )

    let _ =
        outstandingPolicyIdsBuffer.LinkTo (policyIdsBatcher, linkOptions)

    let _ =
        policyIdsBatcher.LinkTo (databaseReaderBlock, linkOptions)

    let _ =
        databaseReaderBlock.LinkTo (parsedRecordProcessorBlock, linkOptions, _.IsOk)

    let _ =
        databaseReaderBlock.LinkTo (failedRecordParseSinkBlock, linkOptions, _.IsError)


    [<EntryPoint>]
    let main _ = 
        use cts =
            new CancellationTokenSource ()

        use _ =
            loggingSubject
            |> Observable.synchronize
            |> Observable.subscribe (printfn "LOG: %s")

        let outstandingPolicyIds =
            Seq.init 250 (sprintf "#%i")

        let insertionLoop =
            backgroundTask {
                do logStr "Starting insertion loop..."

                for polId in outstandingPolicyIds do                        
                    let! _ =
                        outstandingPolicyIdsBuffer.SendAsync polId

                    ()

                do outstandingPolicyIdsBuffer.Complete ()
            }

        let blockDescPairs : (IDataflowBlock * string) list =
            [
                outstandingPolicyIdsBuffer, "Outstanding policy ids"
                policyIdsBatcher, "Policy ID batcher"
                databaseReaderBlock, "Database reader"
                parsedRecordProcessorBlock, "Parsed record processor"
                failedRecordParseSinkBlock, "Failed record parses"
            ]

        blockDescPairs
        |> List.map (fun (block, desc) ->
            block.Completion.ContinueWith (fun _ ->
                do logStr $"{desc} completed."))
        |> ignore

        do  Task.WhenAll(
                parsedRecordProcessorBlock.Completion,
                failedRecordParseSinkBlock.Completion
            )
            |> _.Wait()

        0