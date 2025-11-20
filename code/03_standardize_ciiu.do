/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 03_standardize_ciiu.do
PURPOSE: Harmonize CIIU codes across revisions (Rev 2, 3, 4) to ISIC Rev 4
         
INPUTS:
    - panel_eam_clean.dta (from script 02)
    - ISIC concordance tables from UN Statistics Division
    
OUTPUTS:
    - panel_eam_harmonized.dta (panel with harmonized CIIU Rev 4 codes)
    - ciiu_harmonization_summary.csv (match rates by revision)
    - sample_size_harmonized_2d.csv (observations by 2-digit CIIU Rev 4)
    - sample_size_harmonized_3d.csv (observations by 3-digit CIIU Rev 4)

METHODOLOGY:
    Uses official UN ISIC correspondence tables:
    - Rev 2 -> Rev 3 -> Rev 3.1 -> Rev 4
    - Rev 3 -> Rev 3.1 -> Rev 4
    - Rev 4 codes remain unchanged
    
    For multiple matches, keeps most frequent correspondence
    Manufacturing codes: ISIC Rev 4 divisions 10-33

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

foreach dir in concordance_dir clean_dir output_dir logs_dir {
    capture mkdir "${`dir'}"
}

log using "$logs_dir/03_standardize_ciiu_`c(current_date)'.log", replace text

*==============================================================================*
* PART 1: BUILD ISIC VERSION CONCORDANCES
*==============================================================================*

*------------------------------------------------------------------------------*
* ISIC Rev 2 -> Rev 3
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/ISIC3-ISIC2.txt", clear varnames(1)

keep isic3 isic2
drop if missing(isic3) | missing(isic2)
duplicates drop
destring isic2 isic3, replace force

save "$concordance_dir/isic2_isic3_concordance.dta", replace

*------------------------------------------------------------------------------*
* ISIC Rev 3 -> Rev 3.1
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/ISIC_Rev_31-ISIC_Rev_3_correspondence.txt", clear varnames(1)

rename rev31 isic31
rename rev3 isic3

keep isic3 isic31
drop if missing(isic3) | missing(isic31)
duplicates drop
destring isic3 isic31, replace force

save "$concordance_dir/isic3_isic31_concordance.dta", replace

*------------------------------------------------------------------------------*
* ISIC Rev 3.1 -> Rev 4
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/ISIC4_ISIC31.txt", clear varnames(1)

rename isic4code isic4
rename isic31code isic31

keep isic31 isic4
drop if missing(isic31) | missing(isic4)
duplicates drop
destring isic31 isic4, replace force

save "$concordance_dir/isic31_isic4_concordance.dta", replace

*==============================================================================*
* PART 2: BUILD COMBINED CONCORDANCES TO REV 4
*==============================================================================*

*------------------------------------------------------------------------------*
* Rev 2 -> Rev 4 (via Rev 3 and Rev 3.1)
*------------------------------------------------------------------------------*

use "$concordance_dir/isic2_isic3_concordance.dta", clear

merge m:m isic3 using "$concordance_dir/isic3_isic31_concordance.dta", ///
    keep(match master) nogen

merge m:m isic31 using "$concordance_dir/isic31_isic4_concordance.dta", ///
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

*------------------------------------------------------------------------------*
* Rev 3 -> Rev 4 (via Rev 3.1)
*------------------------------------------------------------------------------*

use "$concordance_dir/isic3_isic31_concordance.dta", clear

merge m:m isic31 using "$concordance_dir/isic31_isic4_concordance.dta", ///
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
* PART 3: HARMONIZE EAM PANEL
*==============================================================================*

use "$clean_dir/panel_eam_clean.dta", clear

*------------------------------------------------------------------------------*
* Identify CIIU revision used in each observation
*------------------------------------------------------------------------------*

gen ciiu_revision = .

* Parse from ciiu_source variable which contains the original variable name
* Examples: "CIIU4AC", "CIIU3AC", "CIIU2N4", "CIUU2", etc.

* Rev 4: Contains "4" after CIIU/CIUU
replace ciiu_revision = 4 if regexm(ciiu_source, "[CI]+U+[24]") & regexm(ciiu_source, "4")

* Rev 3: Contains "3" after CIIU/CIUU
replace ciiu_revision = 3 if regexm(ciiu_source, "[CI]+U+[23]") & regexm(ciiu_source, "3") & missing(ciiu_revision)

* Rev 2: Contains "2" after CIIU/CIUU (but not if already Rev 4)
replace ciiu_revision = 2 if regexm(ciiu_source, "[CI]+U+2") & missing(ciiu_revision)

* If still missing, try to infer from year (EAM documentation)
* Rev 2: 1992-2000 (approximately)
* Rev 3: 2001-2011 (approximately)  
* Rev 4: 2012+ (approximately)
replace ciiu_revision = 2 if missing(ciiu_revision) & year <= 2000
replace ciiu_revision = 3 if missing(ciiu_revision) & year >= 2001 & year <= 2011
replace ciiu_revision = 4 if missing(ciiu_revision) & year >= 2012

label variable ciiu_revision "CIIU/ISIC revision used (2, 3, or 4)"

* Verify we captured revisions
tab ciiu_revision, missing

* Show examples of ciiu_source patterns by revision
preserve
    bysort ciiu_revision: gen n = _n
    keep if n <= 3
    list ciiu_revision ciiu_source year ciiu in 1/9, separator(0) abbreviate(20)
restore

* Keep ciiu as numeric for consistency
gen ciiu_numeric = real(ciiu)
label variable ciiu_numeric "CIIU code (numeric)"

*------------------------------------------------------------------------------*
* Initialize harmonized variables
*------------------------------------------------------------------------------*

gen ciiu_rev4_4d = .
gen ciiu_rev4_3d = .
gen ciiu_rev4_2d = .

label variable ciiu_rev4_4d "ISIC Rev 4 - 4 digit"
label variable ciiu_rev4_3d "ISIC Rev 4 - 3 digit"
label variable ciiu_rev4_2d "ISIC Rev 4 - 2 digit (division)"

*------------------------------------------------------------------------------*
* Rev 4 codes stay unchanged
*------------------------------------------------------------------------------*

replace ciiu_rev4_4d = ciiu_numeric if ciiu_revision == 4
replace ciiu_rev4_3d = floor(ciiu_numeric/10) if ciiu_revision == 4
replace ciiu_rev4_2d = floor(ciiu_numeric/100) if ciiu_revision == 4

*------------------------------------------------------------------------------*
* Merge Rev 2 codes
*------------------------------------------------------------------------------*

preserve
    use "$concordance_dir/ciiu_rev2_to_rev4.dta", clear
    
    keep ciiu_original ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    
    * Create 3-digit and 2-digit versions
    gen ciiu_3d = floor(ciiu_original/10)
    gen ciiu_2d = floor(ciiu_original/100)
    
    * Save complete dataset with all versions
    rename ciiu_original ciiu_4d
    keep ciiu_4d ciiu_3d ciiu_2d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    tempfile rev2_full
    save `rev2_full'
restore

* Create 4-digit concordance
preserve
    use `rev2_full', clear
    keep ciiu_4d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    rename ciiu_4d ciiu_match
    tempfile rev2_4d
    save `rev2_4d'
restore

* Create 3-digit concordance
preserve
    use `rev2_full', clear
    keep ciiu_3d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    duplicates drop ciiu_3d, force
    rename ciiu_3d ciiu_match
    tempfile rev2_3d
    save `rev2_3d'
restore

* Create 2-digit concordance
preserve
    use `rev2_full', clear
    keep ciiu_2d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    duplicates drop ciiu_2d, force
    rename ciiu_2d ciiu_match
    tempfile rev2_2d
    save `rev2_2d'
restore

* For panel data: try matches at different levels
gen ciiu_match = real(ciiu)

* Try 4-digit first
merge m:1 ciiu_match using `rev2_4d', keep(master match) nogen

* Try 3-digit for unmatched
replace ciiu_match = floor(ciiu_match/10) if ciiu_revision == 2 & missing(ciiu_rev4_2d)
merge m:1 ciiu_match using `rev2_3d', keep(master match) nogen update

* Try 2-digit for still unmatched
replace ciiu_match = floor(ciiu_match/10) if ciiu_revision == 2 & missing(ciiu_rev4_2d)
merge m:1 ciiu_match using `rev2_2d', keep(master match) nogen update

drop ciiu_match

*------------------------------------------------------------------------------*
* Merge Rev 3 codes  
* Note: Colombia used CIIU Rev 3 AC (Adapted) with more granular codes than
*       standard ISIC Rev 3. For Colombian-specific 4-digit codes, we map to
*       3-digit ISIC class level.
*------------------------------------------------------------------------------*

preserve
    use "$concordance_dir/ciiu_rev3_to_rev4.dta", clear
    
    keep ciiu_original ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    
    * Create 3-digit version (class level)
    gen ciiu_3d = floor(ciiu_original/10)
    
    * Save complete dataset with both versions
    rename ciiu_original ciiu_4d
    keep ciiu_4d ciiu_3d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    tempfile rev3_full
    save `rev3_full'
restore

* Create 4-digit concordance
preserve
    use `rev3_full', clear
    keep ciiu_4d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    rename ciiu_4d ciiu_match
    tempfile rev3_4d
    save `rev3_4d'
restore

* Create 3-digit concordance for Colombian AC codes
preserve
    use `rev3_full', clear
    keep ciiu_3d ciiu_rev4_4d ciiu_rev4_3d ciiu_rev4_2d
    duplicates drop ciiu_3d, force
    rename ciiu_3d ciiu_match
    tempfile rev3_3d
    save `rev3_3d'
restore

* For panel data: try 4-digit match first
gen ciiu_match = real(ciiu)
merge m:1 ciiu_match using `rev3_4d', keep(master match) nogen

* For unmatched, try 3-digit (for CIIU AC codes)
replace ciiu_match = floor(ciiu_match/10) if ciiu_revision == 3 & missing(ciiu_rev4_2d)
merge m:1 ciiu_match using `rev3_3d', keep(master match) nogen update

drop ciiu_match

*==============================================================================*
* PART 4: QUALITY CHECKS AND SUMMARY STATISTICS
*==============================================================================*

*------------------------------------------------------------------------------*
* Harmonization match rates by source revision
*------------------------------------------------------------------------------*

preserve
    gen harmonized = !missing(ciiu_rev4_2d)
    
    collapse (count) n_total=harmonized ///
             (sum) n_harmonized=harmonized ///
             (mean) match_rate=harmonized, ///
             by(ciiu_revision)
    
    gen pct_harmonized = match_rate * 100
    format match_rate %9.4f
    format pct_harmonized %9.2f
    
    label variable ciiu_revision "Source CIIU revision"
    label variable n_total "Total observations"
    label variable n_harmonized "Successfully harmonized"
    label variable match_rate "Match rate"
    label variable pct_harmonized "Percent harmonized"
    
    list, separator(0) abbreviate(20)
    
    export delimited using "$output_dir/ciiu_harmonization_summary.csv", replace
restore

*------------------------------------------------------------------------------*
* Sample sizes by harmonized 2-digit codes (manufacturing only)
*------------------------------------------------------------------------------*

preserve
    keep if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33
    
    collapse (count) n_obs=firm_id ///
             (mean) mean_year=year, ///
             by(ciiu_rev4_2d)
    
    gsort -n_obs
    
    label variable ciiu_rev4_2d "ISIC Rev 4 - 2 digit"
    label variable n_obs "Number of observations"
    label variable mean_year "Mean year"
    
    list, separator(0) abbreviate(20)
    
    export delimited using "$output_dir/sample_size_harmonized_2d.csv", replace
restore

*------------------------------------------------------------------------------*
* Sample sizes by harmonized 3-digit codes (manufacturing only)
*------------------------------------------------------------------------------*

preserve
    keep if ciiu_rev4_2d >= 10 & ciiu_rev4_2d <= 33
    
    collapse (count) n_obs=firm_id, by(ciiu_rev4_3d)
    
    gsort -n_obs
    
    label variable ciiu_rev4_3d "ISIC Rev 4 - 3 digit"
    label variable n_obs "Number of observations"
    
    export delimited using "$output_dir/sample_size_harmonized_3d.csv", replace
restore

*==============================================================================*
* PART 5: SAVE HARMONIZED PANEL
*==============================================================================*

sort firm_id year

compress
save "$clean_dir/panel_eam_harmonized.dta", replace

log close
