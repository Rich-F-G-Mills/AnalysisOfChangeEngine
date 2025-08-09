
namespace AnalysisOfChangeEngine

open System


(*
Design Decision:
    Why would we need these you ask?
    Given that the Guid type is used throughout, it's not impossible
    that (for example) an extraction UID suddently finds itself being
    used as a run UID. This should (!) reduce the chance of that happening.
    The YouTuber 'Coding Jesus' would be proud of this approach, which he
    refers to as 'strong typing'.
*)
[<NoEquality; NoComparison>]
type RunUid =
    | RunUid of Guid

    member this.Value =
        match this with
        | RunUid uid -> uid

[<NoEquality; NoComparison>]
type StepUid =
    | StepUid of Guid

    member this.Value =
        match this with
        | StepUid uid -> uid

