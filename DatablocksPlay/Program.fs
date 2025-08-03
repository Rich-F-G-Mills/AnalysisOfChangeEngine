
open System
open System.Reactive.Concurrency
open System.Reactive.Disposables
open System.Reactive.Subjects
open System.Threading
open System.Reactive.Linq


module internal Program =

    let syncPrint =
        new MailboxProcessor<_> (fun mbox ->
            async {
                while true do
                    let! msg =
                        mbox.Receive ()

                    do printfn "%s" msg
            }
        )

    do syncPrint.Start ()

    let source =
        new Subject<_> ()

    let scheduler =
        new EventLoopScheduler ()

    let _ =
        source
            .ObserveOn(NewThreadScheduler.Default)
            .Subscribe(fun msg ->
                do syncPrint.Post $"Processing on {Environment.CurrentManagedThreadId}: {msg}"

                do Thread.Sleep (TimeSpan.FromSeconds 5)
            )

    let myThreads =
        Array.init 3 (fun idx ->
            new Thread(new ThreadStart (fun _ ->
                do syncPrint.Post $"Beginning posts from {Environment.CurrentManagedThreadId}"

                do source.OnNext ($"Calling on next with idx {idx} from {Environment.CurrentManagedThreadId}")
                do source.OnNext ($"Calling on next with idx {idx} from {Environment.CurrentManagedThreadId}")
                do source.OnNext ($"Calling on next with idx {idx} from {Environment.CurrentManagedThreadId}")

                do syncPrint.Post $"Finished posts from {Environment.CurrentManagedThreadId}"
            ))
        )

    myThreads
    |> Array.iter _.Start()

    do Thread.Sleep (TimeSpan.FromSeconds 10)