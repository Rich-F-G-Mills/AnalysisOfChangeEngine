
## Project `Common`

This project exposes a number of types used throughout the framework.

Note that code comments can be found throughout the [`Types.fs`](/Types.fs) source file.

It is not expected the users will need to directly access types and logic exposed via the other source files within this project.

### What types are used to represent steps?

All step types implement `IStepHeader` which provides all of:

* `Uid` (`System.Guid`)
* `Title` (`System.String`)
* `Description` (`System.String`)

Steps _generally_ fall into the following categories:

* Those which change from where values are sourced in order to populate the results for a given step.
* Those which change the policy data used for a given step.
* Those which change whether a given policy is included or not.

Below lists the various step types possible. Note the following legend:

* **\(V\)**alidatable
  * Allows user to optionally supply validation logic.
  * The information provided depends on the specific step type being validated. In some cases, logic will be provided information relevant to the **C**urrent or **P**revious step.
  * Each _Step Type_ will have a corresponding member called `Validator` which will have a type signature as per the _Validator Type_ column below. 

  | **Step Type** | **Policy Record** | **Step Results** | **Validator Type** |
  | :-----------: | :---------------: | :--------------: | :----------------: |
  | `OpeningStep` | C | C | `OpeningStepValidator` |
  | `DataChangeStep` | P & C | P & C | `DataChangeStepValidator` |
  | `SourceChangeStep` | C | P & C | `SourceChangeStepValidator` |
  | `ClosingExistingDataStep` | P & C | P & C | `DataChangeStepValidator` |
  | `AddNewRecordsStep` | C | C | `AddNewRecordsStepValidator` |

* **\(S\)ourceable
  * 

* `OpeningStep` (V) -

