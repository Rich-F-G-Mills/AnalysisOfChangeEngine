
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

Depending on the set-up used, these values could be obtained (for example) from a spreadsheet based process or from one or more calls to an API. Although values would likely be aggregated in some way for reporting purposes, the underlying values would be kept at an individual record level, better facilitating any analysis as may be required; this is particularly useful when investigating which records are driving unintuitive resuts for a given step in the walk.

### What are we trying to achieve?

The purpose of this project is to provide a framework that allows for walks, such as that summarised above, to be coded in such a way as to be both transparent and also benefit from the advantaged provided by a compiled language such as F#.