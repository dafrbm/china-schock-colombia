/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 05_build_harmonized_concordances.do
PURPOSE: Build harmonized concordances using UN official tables
         - Harmonize EAM CIIU codes to Rev 4 (Rev 2 -> 3 -> 3.1 -> 4)
         - Build HS -> ISIC Rev 4 concordance (via NAICS 2017)

WORKFLOW:
1. Call R script to convert HS -> NAICS 2017 (using concordance package)
2. Process UN correspondence tables for ISIC version conversions
3. Build NAICS 2017 -> ISIC Rev 4 concordance
4. Harmonize EAM CIIU codes to Rev 4
5. Create final HS -> ISIC Rev 4 concordance at 2, 3, 4 digit levels

INPUTS:
    Concordance files:
    - ISIC3-ISIC2.txt (UN: Rev 2 -> Rev 3)
    - ISIC_Rev_31-ISIC_Rev_3_correspondence.txt (UN: Rev 3 -> Rev 3.1)
    - ISIC4_ISIC31.txt (UN: Rev 3.1 -> Rev 4)
    - 2017_NAICS_to_ISIC_4.xlsx (Census Bureau)

    EAM panel:
    - panel_eam_with_tfp.dta

OUTPUTS:
    - ciiu_rev2_to_rev4_concordance.dta
    - ciiu_rev3_to_rev4_concordance.dta
    - hs6_isic4_concordance.dta (at 2, 3, 4 digit levels)
    - panel_eam_harmonized.dta (EAM with CIIU Rev 4)

AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
capture log close

*------------------------------------------------------------------------------*
* Setup
*------------------------------------------------------------------------------*

global main_dir "C:\Users\dafrb\Desktop\EAM_data\CHINA-SCHOCK"
global raw_dir "$main_dir/raw_data"
global concordance_dir "$raw_dir/concordance"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"
global code_dir "$main_dir/code"

log using "$logs_dir/05_concordances_`c(current_date)'.log", replace text

*==============================================================================*
* PART 1: CALL R SCRIPT FOR HS -> NAICS CONVERSION
*==============================================================================*

display as text _n "=== PART 1: Converting HS to NAICS via R ===" _n

* Check if R output already exists (skip if recent)
capture confirm file "$concordance_dir/hs6_naics2017_concordance.dta"
if _rc != 0 {
    * Call R script
    display "Running R script for HS -> NAICS conversion..."
    shell "C:\Program Files\R\R-4.3.1\bin\Rscript.exe" "$code_dir/hs_to_naics_concordance.R" "$concordance_dir" "$concordance_dir"

    * Verify output was created
    capture confirm file "$concordance_dir/hs6_naics2017_concordance.dta"
    if _rc != 0 {
        display as error "ERROR: R script did not produce output file"
        display as error "Please run manually: Rscript hs_to_naics_concordance.R"
        exit 601
    }
}
else {
    display "HS -> NAICS concordance already exists, skipping R call"
}

*==============================================================================*
* PART 2: PROCESS NAICS 2017 -> ISIC REV 4 CONCORDANCE
*==============================================================================*

display as text _n "=== PART 2: Building NAICS 2017 -> ISIC Rev 4 ===" _n

* Import Census Bureau concordance
import excel "$concordance_dir/2017_NAICS_to_ISIC_4.xlsx", clear firstrow

* Clean variable names
capture rename *, lower
ds
local vars `r(varlist)'

* Identify NAICS and ISIC columns
* Typical format: NAICS code, NAICS title, ISIC code, ISIC title
gen naics2017 = ""
gen isic4 = ""

* Try to identify columns by content
foreach var of local vars {
    capture confirm numeric variable `var'
    if _rc == 0 {
        * Check if looks like NAICS (6 digits starting with 1-9)
        quietly summarize `var'
        if r(min) >= 100000 & r(max) < 1000000 {
            replace naics2017 = string(`var') if naics2017 == ""
        }
        * Check if looks like ISIC (4 digits)
        else if r(min) >= 1000 & r(max) < 10000 {
            replace isic4 = string(`var', "%04.0f") if isic4 == ""
        }
    }
    else {
        * String variable - check format
        quietly count if regexm(`var', "^[0-9]{6}$")
        if r(N) > _N * 0.5 {
            replace naics2017 = `var' if naics2017 == ""
        }
        quietly count if regexm(`var', "^[0-9]{4}$")
        if r(N) > _N * 0.5 {
            replace isic4 = `var' if isic4 == ""
        }
    }
}

keep naics2017 isic4
drop if missing(naics2017) | missing(isic4)
duplicates drop

* Generate ISIC at different digit levels
gen isic4_4d = isic4
gen isic4_3d = substr(isic4, 1, 3)
gen isic4_2d = substr(isic4, 1, 2)

* Destring for merging
destring naics2017, replace force
destring isic4_4d isic4_3d isic4_2d, replace force

* Keep manufacturing (ISIC 10-33)
keep if isic4_2d >= 10 & isic4_2d <= 33

label var naics2017 "NAICS 2017 code"
label var isic4_4d "ISIC Rev 4 (4-digit)"
label var isic4_3d "ISIC Rev 4 (3-digit)"
label var isic4_2d "ISIC Rev 4 (2-digit)"

save "$concordance_dir/naics2017_isic4_concordance.dta", replace

display "NAICS -> ISIC concordance: " _N " pairs"

*==============================================================================*
* PART 3: BUILD HS -> ISIC REV 4 CONCORDANCE
*==============================================================================*

display as text _n "=== PART 3: Building HS -> ISIC Rev 4 ===" _n

* Load HS -> NAICS from R output
use "$concordance_dir/hs6_naics2017_concordance.dta", clear

* Rename and prepare for merge
rename naics2017 naics2017_str
gen naics2017 = real(naics2017_str)
drop naics2017_str hs_revision_used

* Merge with NAICS -> ISIC
merge m:1 naics2017 using "$concordance_dir/naics2017_isic4_concordance.dta", ///
    keep(match master) nogen

* For multiple matches, keep most common ISIC per HS6
bysort hs6 isic4_4d: gen n_matches = _N
bysort hs6: egen max_matches = max(n_matches)
keep if n_matches == max_matches
bysort hs6 (isic4_4d): keep if _n == 1

keep hs6 isic4_4d isic4_3d isic4_2d naics2017

* Rename for clarity
rename isic4_4d ciiu_4d
rename isic4_3d ciiu_3d
rename isic4_2d ciiu_2d

label var hs6 "HS 6-digit product code"
label var ciiu_4d "CIIU Rev 4 (4-digit)"
label var ciiu_3d "CIIU Rev 4 (3-digit)"
label var ciiu_2d "CIIU Rev 4 (2-digit)"
label var naics2017 "NAICS 2017 (intermediate)"

compress
save "$concordance_dir/hs6_isic4_concordance.dta", replace

display "HS6 -> ISIC4 concordance: " _N " HS6 codes"

*==============================================================================*
* PART 4: PROCESS ISIC VERSION CONCORDANCES FOR EAM
*==============================================================================*

display as text _n "=== PART 4: Building ISIC Version Concordances ===" _n

*------------------------------------------------------------------------------*
* ISIC Rev 2 -> Rev 3
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/ISIC3-ISIC2.txt", clear varnames(1) delimiters("\t")

* Clean and rename
capture rename *, lower
ds
local vars `r(varlist)'

* Identify columns (format: ISIC2, ISIC3, partial flag)
gen isic2 = ""
gen isic3 = ""

local col = 1
foreach var of local vars {
    if `col' == 1 {
        tostring `var', replace force
        replace isic2 = `var'
    }
    else if `col' == 2 {
        tostring `var', replace force
        replace isic3 = `var'
    }
    local col = `col' + 1
}

keep isic2 isic3
drop if missing(isic2) | missing(isic3)
duplicates drop

destring isic2 isic3, replace force

save "$concordance_dir/isic2_isic3_concordance.dta", replace

display "ISIC Rev 2 -> Rev 3: " _N " pairs"

*------------------------------------------------------------------------------*
* ISIC Rev 3 -> Rev 3.1
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/ISIC_Rev_31-ISIC_Rev_3_correspondence.txt", clear varnames(1) delimiters("\t")

* Clean and identify columns
capture rename *, lower
ds
local vars `r(varlist)'

gen isic3 = ""
gen isic31 = ""

local col = 1
foreach var of local vars {
    if `col' == 1 {
        tostring `var', replace force
        replace isic31 = `var'
    }
    else if `col' == 2 {
        tostring `var', replace force
        replace isic3 = `var'
    }
    local col = `col' + 1
}

keep isic3 isic31
drop if missing(isic3) | missing(isic31)
duplicates drop

destring isic3 isic31, replace force

save "$concordance_dir/isic3_isic31_concordance.dta", replace

display "ISIC Rev 3 -> Rev 3.1: " _N " pairs"

*------------------------------------------------------------------------------*
* ISIC Rev 3.1 -> Rev 4
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/ISIC4_ISIC31.txt", clear varnames(1) delimiters("\t")

* Clean and identify columns
capture rename *, lower
ds
local vars `r(varlist)'

gen isic31 = ""
gen isic4 = ""

local col = 1
foreach var of local vars {
    if `col' == 1 {
        tostring `var', replace force
        replace isic4 = `var'
    }
    else if `col' == 2 {
        tostring `var', replace force
        replace isic31 = `var'
    }
    local col = `col' + 1
}

keep isic31 isic4
drop if missing(isic31) | missing(isic4)
duplicates drop

destring isic31 isic4, replace force

save "$concordance_dir/isic31_isic4_concordance.dta", replace

display "ISIC Rev 3.1 -> Rev 4: " _N " pairs"

*==============================================================================*
* PART 5: CREATE COMBINED ISIC REV 2/3 -> REV 4 CONCORDANCE
*==============================================================================*

display as text _n "=== PART 5: Combined ISIC Concordance ===" _n

*------------------------------------------------------------------------------*
* Rev 2 -> Rev 4 (via Rev 3 and Rev 3.1)
*------------------------------------------------------------------------------*

use "$concordance_dir/isic2_isic3_concordance.dta", clear

* Merge Rev 3 -> Rev 3.1
merge m:1 isic3 using "$concordance_dir/isic3_isic31_concordance.dta", ///
    keep(match master) nogen

* Merge Rev 3.1 -> Rev 4
merge m:1 isic31 using "$concordance_dir/isic31_isic4_concordance.dta", ///
    keep(match master) nogen

* Keep most common mapping for each Rev 2 code
bysort isic2 isic4: gen n = _N
bysort isic2: egen max_n = max(n)
keep if n == max_n
bysort isic2 (isic4): keep if _n == 1

keep isic2 isic4
rename isic2 ciiu_original
rename isic4 ciiu_rev4

gen ciiu_rev4_4d = ciiu_rev4
gen ciiu_rev4_3d = floor(ciiu_rev4/10)
gen ciiu_rev4_2d = floor(ciiu_rev4/100)

gen source_revision = 2

label var ciiu_original "Original CIIU code"
label var ciiu_rev4 "CIIU Rev 4 (4-digit)"
label var ciiu_rev4_4d "CIIU Rev 4 (4-digit)"
label var ciiu_rev4_3d "CIIU Rev 4 (3-digit)"
label var ciiu_rev4_2d "CIIU Rev 4 (2-digit)"
label var source_revision "Source CIIU revision"

save "$concordance_dir/ciiu_rev2_to_rev4.dta", replace

display "CIIU Rev 2 -> Rev 4: " _N " codes"

*------------------------------------------------------------------------------*
* Rev 3 -> Rev 4 (via Rev 3.1)
*------------------------------------------------------------------------------*

use "$concordance_dir/isic3_isic31_concordance.dta", clear

* Merge Rev 3.1 -> Rev 4
merge m:1 isic31 using "$concordance_dir/isic31_isic4_concordance.dta", ///
    keep(match master) nogen

* Keep most common mapping
bysort isic3 isic4: gen n = _N
bysort isic3: egen max_n = max(n)
keep if n == max_n
bysort isic3 (isic4): keep if _n == 1

keep isic3 isic4
rename isic3 ciiu_original
rename isic4 ciiu_rev4

gen ciiu_rev4_4d = ciiu_rev4
gen ciiu_rev4_3d = floor(ciiu_rev4/10)
gen ciiu_rev4_2d = floor(ciiu_rev4/100)

gen source_revision = 3

save "$concordance_dir/ciiu_rev3_to_rev4.dta", replace

display "CIIU Rev 3 -> Rev 4: " _N " codes"

*==============================================================================*
* PART 6: HARMONIZE EAM PANEL TO CIIU REV 4
*==============================================================================*

display as text _n "=== PART 6: Harmonizing EAM Panel ===" _n

use "$clean_dir/panel_eam_with_tfp.dta", clear

local n_obs = _N
display "Loaded EAM panel: " `n_obs' " observations"

* Identify CIIU revision by source variable
gen ciiu_revision = .
replace ciiu_revision = 2 if inlist(ciiu_source, "CIIU2", "CIIU2N4")
replace ciiu_revision = 3 if ciiu_source == "CIIU3"
replace ciiu_revision = 4 if inlist(ciiu_source, "CIIU4", "CIIU_4")

tab ciiu_revision, missing

* Convert original CIIU to numeric for merging
gen ciiu_original = real(ciiu)

* Initialize harmonized variables
gen ciiu_rev4_4d = .
gen ciiu_rev4_3d = .
gen ciiu_rev4_2d = .

* For Rev 4 codes, use directly
replace ciiu_rev4_4d = ciiu_original if ciiu_revision == 4
replace ciiu_rev4_3d = floor(ciiu_original/10) if ciiu_revision == 4
replace ciiu_rev4_2d = floor(ciiu_original/100) if ciiu_revision == 4

* Merge Rev 2 codes
preserve
    use "$concordance_dir/ciiu_rev2_to_rev4.dta", clear
    keep ciiu_original ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    rename ciiu_rev4_4d rev4_4d_temp
    rename ciiu_rev4_3d rev4_3d_temp
    rename ciiu_rev4_2d rev4_2d_temp
    tempfile rev2_conc
    save `rev2_conc'
restore

merge m:1 ciiu_original using `rev2_conc', keep(master match) nogen

replace ciiu_rev4_4d = rev4_4d_temp if ciiu_revision == 2 & missing(ciiu_rev4_4d)
replace ciiu_rev4_3d = rev4_3d_temp if ciiu_revision == 2 & missing(ciiu_rev4_3d)
replace ciiu_rev4_2d = rev4_2d_temp if ciiu_revision == 2 & missing(ciiu_rev4_2d)
drop rev4_*_temp

* Merge Rev 3 codes
preserve
    use "$concordance_dir/ciiu_rev3_to_rev4.dta", clear
    keep ciiu_original ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    rename ciiu_rev4_4d rev4_4d_temp
    rename ciiu_rev4_3d rev4_3d_temp
    rename ciiu_rev4_2d rev4_2d_temp
    tempfile rev3_conc
    save `rev3_conc'
restore

merge m:1 ciiu_original using `rev3_conc', keep(master match) nogen update

replace ciiu_rev4_4d = rev4_4d_temp if ciiu_revision == 3 & missing(ciiu_rev4_4d)
replace ciiu_rev4_3d = rev4_3d_temp if ciiu_revision == 3 & missing(ciiu_rev4_3d)
replace ciiu_rev4_2d = rev4_2d_temp if ciiu_revision == 3 & missing(ciiu_rev4_2d)
drop rev4_*_temp

* Report harmonization results
quietly count if !missing(ciiu_rev4_4d)
local n_harmonized = r(N)
display _n "Harmonization results:"
display "  Total observations: " `n_obs'
display "  Harmonized to Rev 4: " `n_harmonized' " (" %5.1f 100*`n_harmonized'/`n_obs' "%)"

* Keep manufacturing only (ISIC 10-33)
quietly count if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33 & !missing(ciiu_rev4_2d)
display "  Manufacturing (10-33): " r(N)

* Label new variables
label var ciiu_rev4_4d "CIIU Rev 4 harmonized (4-digit)"
label var ciiu_rev4_3d "CIIU Rev 4 harmonized (3-digit)"
label var ciiu_rev4_2d "CIIU Rev 4 harmonized (2-digit)"
label var ciiu_revision "Original CIIU revision (2, 3, or 4)"

* Save harmonized panel
compress
save "$clean_dir/panel_eam_harmonized.dta", replace

display _n "Saved: panel_eam_harmonized.dta"

*==============================================================================*
* PART 7: SUMMARY AND VALIDATION
*==============================================================================*

display as text _n "=== PART 7: Summary ===" _n

* Sample sizes by harmonized 2-digit CIIU
preserve
    keep if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33
    collapse (count) n_obs=firm_id, by(ciiu_rev4_2d)
    gsort -n_obs
    display "Sample sizes by CIIU Rev 4 (2-digit):"
    list ciiu_rev4_2d n_obs in 1/15, clean noobs
    export delimited using "$output_dir/sample_size_harmonized_2d.csv", replace
restore

* Sample sizes by harmonized 3-digit CIIU
preserve
    keep if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33
    collapse (count) n_obs=firm_id, by(ciiu_rev4_3d)
    gsort -n_obs
    display _n "Sample sizes by CIIU Rev 4 (3-digit):"
    display "Industries with >= 100 obs: " _N
    quietly count if n_obs >= 100
    display "  >= 100 obs: " r(N)
    quietly count if n_obs >= 500
    display "  >= 500 obs: " r(N)
    export delimited using "$output_dir/sample_size_harmonized_3d.csv", replace
restore

display _n "=== Concordance Building Complete ===" _n

log close
