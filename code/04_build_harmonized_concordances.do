/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 04_build_harmonized_concordances.do
PURPOSE: Build harmonized concordances using UN official tables

AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
capture log close

global main_dir "C:\Users\dafrb\Desktop\EAM_data\CHINA-SCHOCK"
global raw_dir "$main_dir/raw_data"
global concordance_dir "$raw_dir/concordance"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"
global code_dir "$main_dir/code"

log using "$logs_dir/04_concordances_`c(current_date)'.log", replace text

*==============================================================================*
* PART 1: HS -> NAICS VIA R
*==============================================================================*

capture confirm file "$concordance_dir/hs6_naics2017_concordance.dta"
if _rc != 0 {
    shell Rscript "$code_dir/hs_to_naics_concordance.R" "$concordance_dir" "$concordance_dir"
}

*==============================================================================*
* PART 2: NAICS 2017 -> ISIC REV 4
*==============================================================================*

import excel "$concordance_dir/2017_NAICS_to_ISIC_4.xlsx", clear firstrow

capture rename *, lower
ds
local vars `r(varlist)'

gen naics2017 = ""
gen isic4 = ""

foreach var of local vars {
    capture confirm numeric variable `var'
    if _rc == 0 {
        quietly summarize `var'
        if r(min) >= 100000 & r(max) < 1000000 {
            replace naics2017 = string(`var') if naics2017 == ""
        }
        else if r(min) >= 1000 & r(max) < 10000 {
            replace isic4 = string(`var', "%04.0f") if isic4 == ""
        }
    }
    else {
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

gen isic4_4d = isic4
gen isic4_3d = substr(isic4, 1, 3)
gen isic4_2d = substr(isic4, 1, 2)

destring naics2017, replace force
destring isic4_4d isic4_3d isic4_2d, replace force

keep if isic4_2d >= 10 & isic4_2d <= 33

save "$concordance_dir/naics2017_isic4_concordance.dta", replace

*==============================================================================*
* PART 3: HS -> ISIC REV 4 CONCORDANCE
*==============================================================================*

use "$concordance_dir/hs6_naics2017_concordance.dta", clear

rename naics2017 naics2017_str
gen naics2017 = real(naics2017_str)
drop naics2017_str hs_revision_used

merge m:1 naics2017 using "$concordance_dir/naics2017_isic4_concordance.dta", ///
    keep(match master) nogen

bysort hs6 isic4_4d: gen n_matches = _N
bysort hs6: egen max_matches = max(n_matches)
keep if n_matches == max_matches
bysort hs6 (isic4_4d): keep if _n == 1

keep hs6 isic4_4d isic4_3d isic4_2d naics2017

rename isic4_4d ciiu_4d
rename isic4_3d ciiu_3d
rename isic4_2d ciiu_2d

compress
save "$concordance_dir/hs6_isic4_concordance.dta", replace

*==============================================================================*
* PART 4: ISIC VERSION CONCORDANCES
*==============================================================================*

* ISIC Rev 2 -> Rev 3
import delimited "$concordance_dir/ISIC3-ISIC2.txt", clear varnames(1) delimiters("\t")

capture rename *, lower
ds
local vars `r(varlist)'

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

* ISIC Rev 3 -> Rev 3.1
import delimited "$concordance_dir/ISIC_Rev_31-ISIC_Rev_3_correspondence.txt", clear varnames(1) delimiters("\t")

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

* ISIC Rev 3.1 -> Rev 4
import delimited "$concordance_dir/ISIC4_ISIC31.txt", clear varnames(1) delimiters("\t")

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

*==============================================================================*
* PART 5: COMBINED ISIC REV 2/3 -> REV 4
*==============================================================================*

* Rev 2 -> Rev 4 (via Rev 3 and Rev 3.1)
use "$concordance_dir/isic2_isic3_concordance.dta", clear

merge m:1 isic3 using "$concordance_dir/isic3_isic31_concordance.dta", ///
    keep(match master) nogen

merge m:1 isic31 using "$concordance_dir/isic31_isic4_concordance.dta", ///
    keep(match master) nogen

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

save "$concordance_dir/ciiu_rev2_to_rev4.dta", replace

* Rev 3 -> Rev 4 (via Rev 3.1)
use "$concordance_dir/isic3_isic31_concordance.dta", clear

merge m:1 isic31 using "$concordance_dir/isic31_isic4_concordance.dta", ///
    keep(match master) nogen

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

*==============================================================================*
* PART 6: HARMONIZE EAM PANEL
*==============================================================================*

use "$clean_dir/panel_eam_with_tfp.dta", clear

* Identify CIIU revision
gen ciiu_revision = .
replace ciiu_revision = 2 if inlist(ciiu_source, "CIIU2", "CIIU2N4")
replace ciiu_revision = 3 if ciiu_source == "CIIU3"
replace ciiu_revision = 4 if inlist(ciiu_source, "CIIU4", "CIIU_4")

gen ciiu_original = real(ciiu)

* Initialize harmonized variables
gen ciiu_rev4_4d = .
gen ciiu_rev4_3d = .
gen ciiu_rev4_2d = .

* Rev 4 codes stay the same
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

compress
save "$clean_dir/panel_eam_harmonized.dta", replace

* Sample size analysis
preserve
    keep if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33
    collapse (count) n_obs=firm_id, by(ciiu_rev4_2d)
    gsort -n_obs
    export delimited using "$output_dir/sample_size_harmonized_2d.csv", replace
restore

preserve
    keep if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33
    collapse (count) n_obs=firm_id, by(ciiu_rev4_3d)
    gsort -n_obs
    export delimited using "$output_dir/sample_size_harmonized_3d.csv", replace
restore

log close
