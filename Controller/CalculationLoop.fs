
namespace AnalysisOfChangeEngine.Controller

open System.Threading.Tasks.Dataflow
open AnalysisOfChangeEngine


[<NoEquality; NoComparison>]
type private CohortedPolicyRecord<'TPolicyRecord> =
    | Exited of 'TPolicyRecord
    | Remaining of 'TPolicyRecord * 'TPolicyRecord
    | New of 'TPolicyRecord

type CalculationLoop<'TPolicyRecord, 'TStepResults>
    (dataStore: IDataStore<'TPolicyRecord, 'TStepResults>) =

    let ouststandingPolicyIdBatcherBlock =
        new BatchBlock<OutstandingPolicyId> (
            100,
            new GroupingDataflowBlockOptions (
                BoundedCapacity = 1000,
                EnsureOrdered = true
            )
        )
(*
    let policyReader outstandingIds =
        backgroundTask {
            let openingPolicyIds =
                
        }

    let policyReaderBlock =
        new TransformManyBlock<OutstandingPolicyId array, CohortedPolicyRecord<'TPolicyRecord>> (
            fun policyIds ->
                dataStore.GetPolicyRecords policyIds)

*)

    


    
    