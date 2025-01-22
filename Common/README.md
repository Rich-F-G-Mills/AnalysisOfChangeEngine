
## Project `Common`

This project exposes a number of types (including interfaces) used throughout the framework.

Note that code comments can be found throughout the [`Types.fs`](/Common/Types.fs) source file.

It is not expected the users will need to directly access types and logic exposed via the other source files within this project.

### Something to be aware of...

Certain types are generic, requiring type parameters to be supplied corresponding to some or all of the following:

* The record type used to reflect policy information (`'TPolicyRecord`). Note that any type used for this must implement `IPolicyRecord` and satisfy the `equality` constraint.
* The record type used to reflect the metrics tracked at each step (`'TSepResults`).
* The record type used to reflect the collection of APIs available for use throughout the walk (`'TApiCollection`).

This would include all of the types used to represent individual steps which are at least generic in terms of `'TPolicyRecord` and `'TSepResults`.

Another would be the abstract base class (`AbstractWalk<...>`) required of all user defined walks which is generic in terms of all three.

### What types are used to represent steps?

All step types implement `IStepHeader` which provides all of:

* `Uid` (`System.Guid`)
* `Title` (`System.String`)
* `Description` (`System.String`)

Steps _generally_ fall into the following categories:

* Those which change from where values are sourced in order to populate the results for a given step.
* Those which change the policy data used for a given step.
* Those which change whether a given policy is included or not.

Before we list the various step types available, please note the following short-hand:

* **`V`**-alidatable
  - Allows user to optionally supply validation logic.
  - The information on which validation can be performed depends on the specific step type being validated. In some cases, logic will be provided information relevant to the **C**urrent or **P**revious step.
  - Each _Step Type_ will have a corresponding member called `Validator` which will have a type signature as per the _Validator Type_ column below. 

      | **Step Type** | **Policy Record** | **Step Results** | **Validator Type** |
      | :-----------: | :---------------: | :--------------: | :----------------: |
      | `OpeningStep` | C | C | `OpeningStepValidator` |
      | `DataChangeStep` | P & C | P & C | `DataChangeStepValidator` |
      | `SourceChangeStep` | C | P & C | `SourceChangeStepValidator` |
      | `ClosingExistingDataStep` | P & C | P & C | `DataChangeStepValidator` |
      | `AddNewRecordsStep` | C | C | `AddNewRecordsStepValidator` |

  - This will be discussed further as part of the [`Implementations`](/Implementations/README.md) project.

* **`S`**-ourceable
  - Requires the user to specify how the metrics for that step are calculated.
  - For those metrics **not** specified, the respective definitions in effect as of the previous step will be used. This allows the user to only specify definitions for a subset of metrics where changes are required.
  - These steps will have a `Source` member of type `SourceExpr<...>`, which requires definitions to follow a particular pattern. This is discussed further as part of the [`Implementations`](/Implementations/README.md) project.

As for the types used to represent the steps themselves:

| **Type Name** | **Capabilities** | **Comments** |
| :-----------: | :--------------: | :----------- |
| `OpeningStep` | `V` | Required type for the very first step in any walk. |
| `SourceChangeStep` | `V` `S` | Required type for the second step in any walk. Can optionally be used for one or more interior steps. |
| `RemoveExitedRecordsStep` | | Required type for the third step in any walk. |
| `DataChangeStep` | `V` | Can optionally be used for one or more interior steps. |
| `ClosingExistingDataStep` | `V` | Required type for the penultimate step in any walk. |
| `AddNewRecordsStep` | `V` | Required type for the final step in any walk. |

