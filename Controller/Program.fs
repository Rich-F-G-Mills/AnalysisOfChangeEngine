
open System
open System.IO
open Microsoft.VisualBasic.FileIO

open FsToolkit.ErrorHandling

open AnalysisOfChangeEngine.Implementations


[<EntryPoint>]
let main _ =
    result {
        let runContext =
            { RunDate = DateOnly.FromDateTime DateTime.Now }

        let streamOptions =
            new FileStreamOptions (Mode = FileMode.Open, Options = FileOptions.SequentialScan)

        use sr =
            new StreamReader ("POLICY_DATA.CSV", streamOptions)

        use tfp =
            new TextFieldParser (sr)

        do tfp.Delimiters <- [| "," |]
        do tfp.TextFieldType <- FieldType.Delimited

        let csvFields =
            tfp.ReadFields ()

        let! rowParser =
            OBWholeOfLife.ValReader.createRowParser runContext csvFields

        while not tfp.EndOfData do
            let row =
                tfp.ReadFields ()

            let parsedRow =
                rowParser row

            do printfn "%A" parsedRow    

        return 0
    }
    |> Result.teeError (printfn "Error: %s")
    |> Result.defaultValue -1

