
## Project `Common`

This project exposes a number of types used throughout the framework.

Note that code comments can be found throughout the [`Types.fs`](/Types.fs) source file.

It is not expected the users will need to directly access types and logic exposed via the other source files within this project.

### What types are used to represent steps?

All step types implement `IStepHeader` which provides all of:

* `Uid` (`System.Guid`)
* `Title` (`System.String`)
* `Description` (`System.String`)

Steps generally fall into the following categories:

* Those which change from where values are sourced in order to populate the results for a given step.
* Those which change the policy data used for a given step.
* Those which change whether a given policy is included or not.

As for the types themselves:

TODO

