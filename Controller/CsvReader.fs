
namespace AnalysisOfChangeEngine.Controller


module CsvReader =

    open System.IO
    open Microsoft.VisualBasic.FileIO
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine.Implementations


    let private streamOptions =
        new FileStreamOptions (Mode = FileMode.Open, Options = FileOptions.SequentialScan)

    let createReaderFromFilePath rowParserFactory skipRows filePath =
        result {
            // This could raise an exception.
            // Given this is done up-front, seems reasonable.
            use sr =
                new StreamReader (filePath, streamOptions)

            // Again, this could fail if not properly formatted.
            use tfp =
                new TextFieldParser (sr)

            do tfp.Delimiters <- [| "," |]
            do tfp.TextFieldType <- FieldType.Delimited

            let columnHeaders =
                tfp.ReadFields ()

            let! rowParser =
                rowParserFactory columnHeaders

            while not tfp.EndOfData do
                let rowArray =
                    tfp.ReadFields ()

                0
                
        }
