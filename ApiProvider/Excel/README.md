## Project `ApiProvider.Excel`

The logic within this project does **not** directly provider `IApiRequestor` instances directly. Instead, it provides instances of an `IExcelDispatcher` which can be used to submit calculations to one or more Excel instances on a first-come-first-served basis.

The approach assumes that any inputs required for a given calculation can be split into those which are specific to either:

- The current step being processed.
- The current policy record being processed.

Behind the scenes, the internally visible `DataTransfer` class provides logic that, when provided with a record type, will generate the logic required to populate a workbook with the values held within such a record instance (a so called _transfer object_).

Cells within an Excel workbook are identified using their range names. Where the name assigned to a member of a transfer object doesn't directly corresponding range name, an `ExcelRangeAlias` attribute must be applied to the member within the original type definition.

At this time, the logic is able to marshall the following types **into** Excel:

- Primitive types as defined by the `primitiveTypes` constant within `DataTransfer.fs`.
- **Non**-parameterised discriminated unions.
- Optional versions of the above.

Required outputs are specified via an array of `PropertyInfo` instances. Typically, when specifying the logic underlying a walk, the user will specify the required outputs by quoting members of a record type which is solely used to provide a 'menu' of available outputs. Given these quotations readily provide `PropertyInfo` instances for those specified items, we also use them here as well.

At this time, only `float32` types can be read **from** Excel. Using anything else will lead to a runtime exception being raised.

The main output of interest is the publicly visible `IExcelDispatcher` interface, with instances provided by the `createExcelDispatcher` function. The primary function of this to provide a load-balanced way to submit calculation requests to one or more Excel instances.

Excel instances are located via the `workbookSelector` predicate as supplied in any call to the function above. For every running Excel instance, where one (and only one) workbook satisfies the predicate, that workbook will be used for any calculation submissions.

The `ExecuteAsync` method exposed by the interface enforces that any assigned Excel instance will process a single request at a time, and that any request will be submitted to the next available instance once all previous requests have been processed.

For the truly interested reader, this load-balancer functionality is provided using the `TPL Dataflow` library.
