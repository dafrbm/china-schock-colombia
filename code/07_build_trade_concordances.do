/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 06_build_trade_concordances.do
PURPOSE: Build concordances between trade classifications (HS6) and industrial 
         classifications (NAICS, ISIC Rev 4)
         
CONCORDANCES BUILT:
    1. HS6 -> NAICS 2017 (using Pierce & Schott + R concordance package)
    2. NAICS 2017 -> ISIC Rev 4 (using UN official tables)
    3. HS6 -> ISIC Rev 4 (combined)

INPUTS:
    - hs_sic_naics_imports_89_123_20240801.dta (Pierce & Schott)
    - 2017_NAICS_to_ISIC_4.xlsx (UN Statistics Division)
    - hs_to_naics_concordance.R (R script for additional concordances)
    
OUTPUTS:
    - hs6_naics2017_concordance.dta
    - naics2017_isic4_concordance.dta
    - hs6_isic4_concordance.dta (main output for trade analysis)

METHODOLOGY:
    For multiple matches, keeps most frequent correspondence
    Manufacturing only: ISIC Rev 4 divisions 10-33

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

foreach dir in concordance_dir clean_dir output_dir logs_dir {
    capture mkdir "${`dir'}"
}

log using "$logs_dir/06_trade_concordances_`c(current_date)'.log", replace text

*==============================================================================*
* PART 1: PROCESS PIERCE & SCHOTT HS -> NAICS
*==============================================================================*

use "$concordance_dir/hs_sic_naics_imports_89_123_20240801.dta", clear

*------------------------------------------------------------------------------*
* Extract HS6 codes preserving leading zeros
*------------------------------------------------------------------------------*

tostring commodity, gen(hs10_str) format(%010.0f) force
gen hs6_str = substr(hs10_str, 1, 6)

keep hs6_str naics year
drop if missing(hs6_str) | missing(naics) | hs6_str == "000000"

*------------------------------------------------------------------------------*
* Keep most frequent HS6-NAICS combinations
*------------------------------------------------------------------------------*

bysort hs6_str naics: gen n_obs = _N
bysort hs6_str: egen max_obs = max(n_obs)
keep if n_obs == max_obs

duplicates drop hs6_str naics, force

*------------------------------------------------------------------------------*
* Save temp file for R concordance package
*------------------------------------------------------------------------------*

preserve
    keep hs6_str naics
    duplicates drop
    rename hs6_str hs6
    save "$concordance_dir/hs6_naics_temp.dta", replace
restore

*==============================================================================*
* PART 2: HS -> NAICS VIA R CONCORDANCE PACKAGE
*==============================================================================*

shell "C:\Program Files\R\R-4.5.1\bin\Rscript.exe" "$code_dir/hs_to_naics_concordance.R" "$concordance_dir" "$concordance_dir"

*==============================================================================*
* PART 3: NAICS 2017 -> ISIC REV 4
*==============================================================================*

import excel "$concordance_dir/2017_NAICS_to_ISIC_4.xlsx", clear firstrow
capture rename *, lower

*------------------------------------------------------------------------------*
* Parse NAICS and ISIC codes
*------------------------------------------------------------------------------*

gen naics2017 = string(naicsus, "%06.0f")
gen isi = real(isic40)
gen isic4 = string(isi, "%04.0f")

keep naics2017 isic4
drop if missing(naics2017) | missing(isic4)
duplicates drop

*------------------------------------------------------------------------------*
* Create multi-level ISIC codes
*------------------------------------------------------------------------------*

gen isic4_4d = isic4
gen isic4_3d = substr(isic4, 2, 3)
gen isic4_2d = substr(isic4, 2, 2)

*------------------------------------------------------------------------------*
* Keep manufacturing sectors only (ISIC Rev 4 divisions 10-33)
*------------------------------------------------------------------------------*

destring isic4_2d, generate(isic4_2d_num) force
keep if isic4_2d_num >= 10 & isic4_2d_num <= 33
drop isic4_2d_num

label variable naics2017 "NAICS 2017 code"
label variable isic4_4d "ISIC Rev 4 - 4 digit"
label variable isic4_3d "ISIC Rev 4 - 3 digit"
label variable isic4_2d "ISIC Rev 4 - 2 digit"

save "$concordance_dir/naics2017_isic4_concordance.dta", replace

*==============================================================================*
* PART 4: BUILD COMPLETE HS6 -> ISIC REV 4 CONCORDANCE
*==============================================================================*

use "$concordance_dir/hs6_naics2017_concordance.dta", clear

*------------------------------------------------------------------------------*
* Merge with NAICS -> ISIC concordance
*------------------------------------------------------------------------------*

merge m:m naics2017 using "$concordance_dir/naics2017_isic4_concordance.dta", ///
    keep(match master) nogen

*------------------------------------------------------------------------------*
* Resolve multiple matches (keep most frequent)
*------------------------------------------------------------------------------*

bysort hs6 isic4_4d: gen n_matches = _N
bysort hs6: egen max_matches = max(n_matches)
keep if n_matches == max_matches
bysort hs6 (isic4_4d): keep if _n == 1

*------------------------------------------------------------------------------*
* Rename to Colombian CIIU convention
*------------------------------------------------------------------------------*

keep hs6 isic4_4d isic4_3d isic4_2d naics2017

rename isic4_4d ciiu_rev4_4d
rename isic4_3d ciiu_rev4_3d
rename isic4_2d ciiu_rev4_2d

label variable hs6 "Harmonized System 6-digit code"
label variable ciiu_rev4_4d "CIIU Rev 4 - 4 digit"
label variable ciiu_rev4_3d "CIIU Rev 4 - 3 digit"
label variable ciiu_rev4_2d "CIIU Rev 4 - 2 digit"
label variable naics2017 "NAICS 2017 code"

*------------------------------------------------------------------------------*
* Summary statistics
*------------------------------------------------------------------------------*

preserve
    collapse (count) n_hs6=hs6, by(ciiu_rev4_2d)
    gsort -n_hs6
    
    label variable ciiu_rev4_2d "CIIU Rev 4 - 2 digit"
    label variable n_hs6 "Number of HS6 codes"
    
    list, separator(0) abbreviate(20)
    
    export delimited using "$output_dir/hs6_by_ciiu2d.csv", replace
restore

*------------------------------------------------------------------------------*
* Save final concordance
*------------------------------------------------------------------------------*

compress
save "$concordance_dir/hs6_isic4_concordance.dta", replace

log close
