
namespace AnalysisOfChangeEngine.Common


[<RequireQualifiedAccess>]
module RowParser =

    open System.Collections.Generic
    open FsToolkit.ErrorHandling
    

    let createRowParser<'TSplitRow, 'TPolicyRecord>
        ((splitterFactory: string seq -> Result<string array -> Result<'TSplitRow, string>, string>),
         (policyIdGetter: 'TSplitRow -> string),
         (splitRowParser: (CleansingChange -> unit) * 'TSplitRow -> Result<'TPolicyRecord, string>))
        availableHeaders: Result<string array -> ParseOutcome<_>, _> =
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
                            // Here we don't have any policy ID to assign to the error.
                            |> Result.mapError (fun err -> None, err)

                        let! parsedRow =
                            splitRowParser (cleansingChanges.Add, splitRow)
                            // ...But here we do!
                            |> Result.mapError (fun err -> Some (policyIdGetter splitRow), err)

                        return parsedRow, Seq.toList cleansingChanges
                    }

                return rowParser                
            }
        