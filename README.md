
## Analysis Of Change Engine

### Some context...

Insurance companies (and likely other financial institutions) need to provide regular MI on the portfolio of business held on their books. This could be done at whatever frequency is desired, with values provided at the end of each respective **reporting period**.

The date at which these values are deemed to be effective is (not surprisingly) called the **effective date**.

In the case of an insurer, this regular MI could inlude (for example) the total asset share of this business which represents a fair value on which claim values can be based.

One simple approach is to simply calculate these metrics at the end of each reporting period. However, this doesn't contribute much information in terms of what has driven changes in these values nor provide enough information to give comfort that such movements are commensurate with expectations.

One solution to this is to perform an **analysis of change** (**AoC**). This approach aims to break down the change in values between reporting periods, for each record in the portfolio, by considering changes in the factors underlying those values.

In the case of life insurance business, one approach would be to calculate these values (such as asset shares) corresponding to each of the following steps, noting that each **step** in this **walk** is applied cumulatively.

* **Opening position** - Values from the end of the prior reporting period.
* **Opening re-run** - Re-run the latest logic using the data and parameters used at that time. In the absence of any changes in logic, this step provides a regression test on production logic.
* **Remove exited business** - Quantify the impact of any business that has left over the latest reporting period.
* **Data restatements** - Updating our opening data for any **unexpected** changes made over the reporting period. This would not typically include changes that would typically occur due to the passing of time (eg. date of last premium paid).
* **Opening restatements** - Recalculate opening values reflecting our latest view of parameters such as investment returns and deductions.
* **Data roll-forward** - Update our data for changes that typically occur due to the passing of time.
* **Change in effective date** - Move the date at which our values are effective from the end of the prior reporting period to that of the current reporting period.
* **Subsequent data and parameter changes** - Separate steps would be used to quantify the impact of (for example) premiums, deductions and investment returns.
* **New/reinstated business** - Allows us to quantify the impact of and new records in the portfolio. This is usually taken to be the closing step.

Depending on the set-up used, these values could be obtained (for example) from a spreadsheet based process or from one or more calls to an API. Although values would likely be aggregated in some way for reporting purposes, keeping the underlying record-level values better facilitates any analysis as may be required; this is particularly useful when investigating which records are driving unintuitive resuts for a given step in the walk.

### What are we trying to achieve here?

The purpose of this repository, and the various projects within, is to provide a framework that allows for walks (such as that summarised above) to be coded in such a way as to be both transparent and also benefit from the advantages provided (e.g. type-safety) by a language such as F#.

Ideally:

* Users should only care about how each step in the walk is defined, and **not** how the machinery makes it happen.
* There should be a transparent way of specifying the logic of each step through a combination of user defined functions and code quotations.
* The number of API calls should be kept to a minimum. Either by re-using already provided values or by requesting as many outputs as needed in each request.

### So... What is provided?

The following projects are **core** to what we are trying to achieve here:

* [**`Common`**](/Common/README.md) - Provides definitions of common types used throughout the solution. It is not expected that users will need to directly reference this project.
* **`Controller`** - Provides logic needed to convert a use supplied walk into actionable code and carry out selected walks.

The following projects are specific to the walks (and hence likely the coresponding portfolio of business) that need to be run and the mechanisms needed to make that happen:

* **`Implementations`** - Definitions of the various user supplied walks, including the building blocks that made that happen.
* **`DataStore.Postgres`** - Provides an interface to an underlying data-store hosted on a Postgres database.
* **`Runner`** - A user supplied executable that, by bringing together the various aspects above, will phyically run the walk.

### Next steps...






### What about the steps themselves?

Although there is no limit to the number of steps in the walk, there _is_ a finite number of step **types** that can appear. Some of these can only be used at specific points (eg. the 'Remove exited business' step seen below). It is these step types that indicate to the machinery what type of action needs to be taken.

All steps implement the `IStepHeader` interface which requires the following information to be made available:

| **Member Name** | **.NET Type** | Comments |
| :-------------: | :-----------: | :------- |
| `Uid` | `System.Guid` | Duplicate `Uid`s within a walk a not permitted. |
| `Title` | `System.String` | |
| `Description` | `System.String` | |

Outside of this, different step types require different information in order to function, as we will see below.

For the **required** steps, we have:

| **Step** | **`AbstractWalk` Member** | **.NET Type** | **Information Required** |
| :------: | :-----------------------: | :-----------: | :----------------------- |
| Opening | `Opening` | `OpeningStep` | None. |
| Opening re-run | `OpeningRegression` | `SourceChangeStep` | See 'Source Steps' below. |
| Remove exited business | `RemoveExitedRecords` | `RemoveExitedRecordsStep` | None. |
| Move to closing existing data | `MoveToClosingExistingData` | `ClosingExistingDataStep` | Optional validation logic. |
| Add new records | `AddNewRecords` | `AddNewRecordsStep` | Optional validation logic. |

For user supplied interior steps, the only choices are:

| **.NET Type** | **Information Required** |
| :-----------: | :----------------------- |
| `SourceChangeStep` | See 'Source Steps' below. |
| `DataChangeStep` | See 'Data Change Steps' below. |

Ultimately, each step/step type indicates one of the following:

* The opening step in the walk.
* Change to the current policy record being processed.
* Change in how the step result metrics are calculated.
  * This could include changes in metrics 