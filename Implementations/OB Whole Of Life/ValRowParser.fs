
namespace AnalysisOfChangeEngine.Implementations.OBWholeOfLife


[<RequireQualifiedAccess>]
module ValRowParser =

    open FsToolkit.ErrorHandling

    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.TypeProviders


    // --- TYPES ---
    type private ValRow = SplitRowType<
        RequiredColumns="""
            POLICY_NUMBER,
            TABLE_NUMBER,
            START_DATE,
            NPDD,
            LPTERM->LIMITED_PAYMENT_TERM,
            POLICY_STATUS,
            ENTRY_AGE_L1,
            GENDER_L1,
            ENTRY_AGE_L2,
            GENDER_L2,
            JVA->JOINT_VALUATION_AGE""">

    type Configuration =
        {
            ``Assumed premium paying term if PUP and missing NPDD``: int
        }

        /// Default configuration:
        ///  * Premiums assumed paid for 20 years if missing NPDD on PUP.
        static member val Default =
            {
                ``Assumed premium paying term if PUP and missing NPDD`` = 20
            }

    // Ideally we'd want the type above to expose an interface, avoiding the need
    // for a separately defined ID getter function!
    let private getPolicyID (valRow: ValRow) =
        valRow.POLICY_NUMBER


    // --- PARSERS ---
    // Here we define a number of parsers needed to extract fields from the VAL file.
    let private parseGender =
        Gender.createParser ("M", "F")

    let private parseOptionalGender =
        Result.makeOptionalParser parseGender

    let private parsePolicyStatus =
        PolicyStatus.createParser ("PP", "PUP", "AP")

    let private parseFlexibleDateOnly str =
        Result.parseISODateOnly str
        |> Result.orElseWith (fun _ -> Result.parseDMYDateOnly str)

    // Assume that we're more likely to have an ISO formatted date before trying DMY.
    let private parseOptionalFlexibleDateOnly str =
        Result.parseOptionalISODateOnly str
        |> Result.orElseWith (fun _ -> Result.parseOptionalDMYDateOnly str)

    let private (|ReTableNumber|_|) =
        (|ReMatch|_|) "^([0-9]{3})([A-Z]?)$"

    let private splitTableNumber = function
        | ReTableNumber (tableNumber, _) ->
            Ok tableNumber
        | tableNumber ->
            Error tableNumber

    let private taxableTableNumbers =
        set [ "001A"; "005C" ]


    // --- PREDICATES ---
    // Here we define any commonly used predicates when validating values from VAL fields.
    let inline private isNonNegative x =
        x >= 0


    // --- ROW PARSER ---
    // The following converts the raw VAL content into a cleansed record.
    let private parseSplitRow (config: Configuration, runContext: RunContext) (notifyChange: CleansingChange -> unit, row: ValRow) =
        result {
            // --- VALIDATE CONFIGURATION ---
            do! Result.requireTrue
                    "Cannot assume negative number of years paid!"
                    (config.``Assumed premium paying term if PUP and missing NPDD`` > 0)


            // --- PROCESS RAW ROW CONTENT ---
            let! entryDate =
                parseFlexibleDateOnly row.START_DATE
                |> Result.mapError (sprintf "Unable to parse start date '%s'")

            and! tableNumber =
                splitTableNumber row.TABLE_NUMBER
                |> Result.mapError (sprintf "Unable to parse table number '%s'")

            and! npdd =
                parseOptionalFlexibleDateOnly row.NPDD
                |> Result.mapError (sprintf "Unable to parse NPDD '%s'")

            and! limitedPaymentTerm =
                Result.parseInt row.LIMITED_PAYMENT_TERM
                |> Result.mapError (sprintf "Unable to parse limited payment term '%s'")

            and! policyStatus =
                parsePolicyStatus row.POLICY_STATUS
                |> Result.mapError (sprintf "Unable to parse policy status '%s'")

            and! entryAge1 =
                Result.parseInt row.ENTRY_AGE_L1
                |> Result.mapError (sprintf "Unable to parse life 1 entry age '%s'")
                |> Result.requireThat isNonNegative "Cannot have negative life 1 entry age."

            and! gender1 =
                parseGender row.GENDER_L1
                |> Result.mapError (sprintf "Unable to parse life 1 gender '%s'")

            and! entryAge2 =
                Result.parseOptionalInt row.ENTRY_AGE_L2
                |> Result.mapError (sprintf "Unable to parse life 2 entry age '%s'")

            and! gender2 =
                parseOptionalGender row.GENDER_L2
                |> Result.mapError (sprintf "Unable to parse life 2 gender '%s'")

            and! jointValuationAge =
                Result.parseOptionalInt row.JOINT_VALUATION_AGE
                |> Result.mapError (sprintf "Unable to parse joint valuation age '%s'")


            // --- MISSING NPDDs ---

            // If we need to insert an NPDD, the approach depends on the policy status.
            let npdd =
                match policyStatus, npdd with
                // If we have an NPDD in the underlying data, we don't need to do anything else.
                | _, Some npdd -> npdd

                // If we're missing it and are PP, take it to be the run date.
                | PolicyStatus.PremiumPaying, None ->
                    do notifyChange {
                        Field = "Next Premium Due Date"
                        From = "NULL"
                        To = runContext.RunDate.ToShortDateString()
                        Reason = "Missing NPDD; using run date as policy is PP"
                    }

                    runContext.RunDate

                // If missing and paid-up, assume a fixed number of years are the DOE.
                | PolicyStatus.PaidUp, None ->
                    let newNpdd =
                        entryDate.AddYears 20

                    do notifyChange {
                        Field = "Next Premium Due Date"
                        From = "NULL"
                        To = newNpdd.ToShortDateString()
                        Reason = "Missing NPDD; assuming premiums paid for 20 years as policy is PUP"
                    }

                    newNpdd

                // If missing and all-paid, assume a fixed number of years are the DOE.
                | PolicyStatus.AllPaid, None ->
                    let newNpdd =
                        entryDate.AddYears limitedPaymentTerm

                    do notifyChange {
                        Field = "Next Premium Due Date"
                        From = "NULL"
                        To = newNpdd.ToShortDateString()
                        Reason = "Missing NPDD; assuming premiums paid until contractual end as all-paid"
                    }

                    newNpdd


            // --- DETERMINE LIVES BASIS ---

            let life1 =
                { EntryAge = entryAge1; Gender = gender1 }

            let! livesBasis =
                match entryAge2, gender2 with
                | Some entryAge2', Some gender2' ->
                    let life2 =
                        { EntryAge = entryAge2'; Gender = gender2' }

                    let jointValuationAge =
                        match jointValuationAge with
                        | None ->
                            let newJVA =
                                (entryAge1 + entryAge2') / 2

                            do notifyChange {
                                Field = "Joint Valuation Age"
                                From = "NULL"
                                To = string newJVA
                                Reason = "Missing JVA; using estimate"
                            }

                            newJVA

                        | Some jva -> jva                            

                    Ok (JointLife (life1, life2, jointValuationAge))

                | None, None ->
                    Ok (SingleLife life1)

                | Some _, None ->
                    Error "Life 2 entry age specified with no corresponding gender"

                | None, Some _ ->
                    Error "Life 2 gender specified with no corresponding entry age"
                    
                    
            // --- RETURN COMPLETED POLICY RECORD ---

            return {
                PolicyNumber = row.POLICY_NUMBER
                EntryDate = entryDate
                NextPremiumDueDate = npdd
                PolicyStatus = policyStatus
                LivesBasis = livesBasis
                PaymentTerm = limitedPaymentTerm
                IsTaxable = taxableTableNumbers.Contains tableNumber
            }
        }


    let create (config, runContext, csvHeaders) =
        RowParser.createRowParser (ValRow.CreateSplitter, getPolicyID, parseSplitRow (config, runContext)) csvHeaders
        