
## Project `Common`

This project exposes a number of types (including interfaces) used throughout the framework.

Note that (hopefully useful) code comments can be found throughout the various source files.


### Something to be aware of...

Certain types are generic, requiring type parameters to be supplied corresponding to some or all of the following:

* The record type used to reflect policy information (`'TPolicyRecord`).
* The record type used to reflect the metrics tracked at each step (`'TStepResults`).
* The record type used to reflect the collection of APIs available for use throughout the walk (`'TApiCollection`).

This would include all of the types used to represent individual steps which are at least generic in terms of `'TPolicyRecord` if not `'TSepResults` as well.

Another would be the abstract base class (`AbstractWalk<...>`) required of all user defined walks which is generic in terms of all three.

### What types are used to represent steps?

All step types implement `IStepHeader` which provides all of:

* `Uid` (`System.Guid`)
* `Title` (`System.String`)
* `Description` (`System.String`)

Steps _generally_ fall into the following categories:

* Those which change from where values are sourced in order to populate the results for a given step, referred to as **source changers**.
* Those which change the policy data used for a given step, referred to as **data changers**.
* Those which change whether a given policy is included or not.

Before we list the various step types available, please note the following short-hand:

* **`V`**-alidatable
  - Allows user to optionally supply validation logic.
  - The information on which validation can be performed depends on the specific step type being validated. In some cases, logic will be provided information relevant to the **C**urrent or **P**revious step.
  - Each _Step Type_ will have a corresponding member called `Validator` which will have a type signature as per the _Validator Type_ column below. 

      | **Step Type** | **Policy Record** | **Step Results** | **Validator Type** |
      | :-----------: | :---------------: | :--------------: | :----------------: |
      | `OpeningReRunStep` | C | C | `OpeningReRunStepValidator` |
      | `DataChangeStep` | P & C | P & C | `DataChangeStepValidator` |
      | `SourceChangeStep` | C | P & C | `SourceChangeStepValidator` |
      | `MoveToClosingDataStep` | P & C | P & C | `DataChangeStepValidator` |
      | `AddNewRecordsStep` | C | C | `AddNewRecordsStepValidator` |

* **`S`**-ourceable
  - Requires the user to specify how the metrics for that step are calculated.
  - For those metrics **not** specified, the respective definitions in effect as of the previous step will be **recalculated**; this distinction is important, especially for those metrics that are calculated with respect to other metrics. This allows the user to only specify definitions for a subset of metrics where changes are required.
  - These steps will have a `Source` member of type `SourceExpr<...>`, which requires definitions to follow a particular pattern.

As for the types used to represent the steps themselves:

| **Type Name** | **Capabilities** | **Required?** | **Comments** |
| :-----------: | :--------------: | :----------- |
| `OpeningReRunStep` | `V` | Yes | Forms the opening step of the walk, and effectively acts as a (regression) re-run of the prior run's closing step. |
| `RemoveExitedRecordsStep` | | Yes | Required type for the third step in any walk. |
| `MoveToClosingDataStep` | `V` | Yes | Represents a move to the closing policy data. |
| `AddNewRecordsStep` | `V` | Yes | Indicates that any new records should now be included within the walk. |
| `SourceChangeStep` | `V` `S` | No | Used to represent a change in definition for one or more step result metrics. |
| `DataChangeStep` | `V` | No | Used to update the policy data being used from this step onwards. |
