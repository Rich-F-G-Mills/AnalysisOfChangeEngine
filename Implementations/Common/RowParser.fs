
namespace AnalysisOfChangeEngine.Implementations


[<RequireQualifiedAccess>]
module RowParser =

    open System.Collections.Generic
    open FsToolkit.ErrorHandling
    

    let createRowParser<'TSplitRow, 'TPolicyRecord>
        ((splitterFactory: string seq -> Result<string array -> Result<'TSplitRow, string>, string>),
         (policyIdGetter: 'TSplitRow -> string),
         (splitRowParser: (CleansingChange -> unit) * 'TSplitRow -> Result<'TPolicyRecord, string>))
        availableHeaders =
            result {
                let! rowSplitter =
                    splitterFactory availableHeaders

                let rowParser row =
                    // Here we use the result monad as a proxy for our desired ParseOutcome return type.
                    result {
                        let cleansingChanges =
                            new List<CleansingChange> ()

                        let! splitRow =
                            rowSplitter row
                            |> Result.mapError ParseOutcome.ReadFailure

                        let! parsedRow =
                            splitRowParser (cleansingChanges.Add, splitRow)
                            |> Result.mapError (fun msg ->
                                ParseOutcome.ParseFailure (policyIdGetter splitRow, msg))

                        return ParseOutcome.Successful (parsedRow, Seq.toList cleansingChanges)                    
                    }
                    |> Result.either id id

                return rowParser                
            }
        