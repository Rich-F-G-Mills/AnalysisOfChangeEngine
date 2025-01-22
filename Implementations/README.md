
## Project `Implementations`

This project provides both:

* Building blocks that make it easier to define steps within a walk (and hence the walk itself).
* User implementations of specific walks.


### So... How is a walk specified?

Each walk is tightly coupled with the following (as defined in the top-level `README`)

* **Step results structure**
* **Policy record structure**
* **API collection**

The walk is made aware of the above via generic type parameters (which must therefore be known at compile-time).

![](/Documentation/classlayout.png)

Walks are defined within the context of a .NET class. In **all** cases, such classes must inherit from the `AbstractWalk` class which enforces the presence of certain steps along with the ability to "register" user supplied steps over and above those required below. 

The first three steps of a walk **must** be:

* Opening position (`OpeningStep`).
* Opening re-run (`SourceChangeStep`).
* Remove exited business (`RemoveExitedRecordsStep`).

The last two steps of a walk **must** be:

* Move to closing details for each record; specifically, those records in-force at **both** the opening **and** closing positions (`ClosingExistingDataStep`).
* Allow for new/reinstated business; also the closing step in the walk (`AddNewRecordsStep`).

The user is free to supply as many additional (ie. **interior**) steps between those above as needed; this is discussed further in the following section.


 similar to that shown below. Note the use of a lambda definition within a code quotation.
  - The table below shows the arguments that will be supplied to (and hence expected of) the quoted lambda. Unused parameters can be replaced with a `_` discarding placeholder. 
    
    | **Index** | **Suggested Name** | **Type** | **Usage** |
    | :-------: | :----------------: | :------: | :-------- |
    | #0 | `from` | `SourceAction<...>` | Provides access to APIs as included within the supplied `'ApiCollection` type parameter. |
    | #1 | `policyRecord` | `'TPolicyRecord` | Provides access to the polcy details as applicable to the current step. |
    | #2 | `priorResults` | `'TStepResults` | This **cannot** be used within a metric definition. It can only be used as part of a wider object expression used to define a new source as a modified version of the source in-effect for the previous step. |
    | #3 | `currentResults` | `'TStepResults` | This can only be used within metric definitions. This allows individual metrics to be expressed in terms of other metrics for the current step. Circular references are not permitted and will lead to a runtime exception if encountered. |

    ```
      ...
      Source = <@
          fun from policyRecord _ currentResults ->
              {
                  stepMetric1 = ...
                  stepMetric2 = ...
                  ...
                  stepMetricN = ...
              } @>
    ```

  - A subset of metrics can be redefined with the following approach:

    ```
      ...
      Source = <@
          fun from policyRecord priorResults currentResults ->
              { priorResults with
                  stepMetricX = ...
                  stepMetricY = ...
                  ...
              } @>
    ```