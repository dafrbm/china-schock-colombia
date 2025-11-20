/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 01_merge_eam.do
PURPOSE: Construct balanced panel from Annual Manufacturing Survey (EAM)
         data files 1992-2023 with harmonized industry classification

INPUTS:
    - EAM_[year].dta files (1992-2023) in raw_data folder
    
OUTPUTS:
    - panel_eam_1992_2023.dta (main panel dataset)
    - common_variables_list.csv (variables available across all years)
    - panel_summary_by_year.csv (panel structure by year)
    - variables_dictionary.csv (data dictionary)

METHODOLOGY:
    1. Identify common variables across all available years
    2. Append years using fuzzy matching for CIIU codes
    3. Harmonize industry classification (CIIU) across different versions
    4. Create unique firm identifier (firm_id)
    5. Generate panel structure variables (entry, exit, balanced)

NOTES:
    - CIIU harmonization handles multiple CIIU versions across years
    - Fuzzy matching used due to inconsistent variable naming
    - Panel is unbalanced by design (firm entry/exit is relevant)

AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
set maxvar 10000

*------------------------------------------------------------------------------*
* Setup
*------------------------------------------------------------------------------*

global main_dir "C:\Users\dafrb\Desktop\EAM_data\CHINA-SCHOCK"
global raw_dir "$main_dir/raw_data"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"

log using "$logs_dir/panel_build_`c(current_date)'.log", replace text

*------------------------------------------------------------------------------*
* Identify available years and common variables
*------------------------------------------------------------------------------*

local year_start = 1992
local year_end = 2023

* Check which years are available
local years_available ""
forvalues year = `year_start'/`year_end' {
    capture confirm file "$raw_dir/eam/EAM_`year'.dta"
    if _rc == 0 {
        local years_available "`years_available' `year'"
    }
}

local n_years : word count `years_available'

* Find intersection of variables across all years
local first_year : word 1 of `years_available'
use "$raw_dir/eam/EAM_`first_year'.dta", clear
rename *, upper
ds
local common_vars `r(varlist)'

foreach year of local years_available {
    if `year' == `first_year' continue
    
    use "$raw_dir/eam/EAM_`year'.dta", clear
    rename *, upper
    ds
    local vars_year `r(varlist)'
    
    local common_vars : list common_vars & vars_year
}

* Save common variables list for reference
clear
set obs 1
gen variables_comunes = "`common_vars'"
export delimited using "$clean_dir/common_variables_list.csv", replace

*------------------------------------------------------------------------------*
* Append years with CIIU harmonization
*------------------------------------------------------------------------------*

clear all
tempfile panel
local year_count = 0

foreach year of local years_available {
    
    use "$raw_dir/eam/EAM_`year'.dta", clear
    local obs_original = _N
    
    rename *, upper
    
    * Fuzzy matching for CIIU variables
    * Variable names highly inconsistent across years
    
    * Initialize CIIU holder variables
    capture confirm variable CIIU_4D
    if _rc != 0 gen CIIU_4D = ""
    else {
        capture confirm string variable CIIU_4D
        if _rc != 0 tostring CIIU_4D, replace
    }
    
    capture confirm variable CIIU_3D
    if _rc != 0 gen CIIU_3D = ""
    else {
        capture confirm string variable CIIU_3D
        if _rc != 0 tostring CIIU_3D, replace
    }
    
    capture confirm variable CIIU_2D
    if _rc != 0 gen CIIU_2D = ""
    else {
        capture confirm string variable CIIU_2D
        if _rc != 0 tostring CIIU_2D, replace
    }
    
    * Handle generic CIIU variable (common in some years like 2001)
    * Must be processed BEFORE the fuzzy search to avoid conflicts
    capture confirm variable CIIU
    if _rc == 0 {
        tempvar ciiu_generic
        capture confirm string variable CIIU
        if _rc == 0 {
            gen `ciiu_generic' = CIIU
        }
        else {
            tostring CIIU, gen(`ciiu_generic')
        }
        
        * Clean and assign to appropriate level based on length
        replace `ciiu_generic' = trim(`ciiu_generic')
        replace `ciiu_generic' = "" if inlist(`ciiu_generic', ".", "0", " ")
        
        * Determine level by actual data length
        quietly count if !missing(`ciiu_generic')
        if r(N) > 0 {
            gen ciiu_len = length(`ciiu_generic')
            quietly summarize ciiu_len if !missing(`ciiu_generic'), detail
            local median_len = r(p50)
            drop ciiu_len
            
            * Assign based on median length of non-missing values
            if `median_len' >= 4 {
                replace CIIU_4D = `ciiu_generic' if missing(CIIU_4D) & !missing(`ciiu_generic')
            }
            else if `median_len' == 3 {
                replace CIIU_3D = `ciiu_generic' if missing(CIIU_3D) & !missing(`ciiu_generic')
            }
            else if `median_len' == 2 {
                replace CIIU_2D = `ciiu_generic' if missing(CIIU_2D) & !missing(`ciiu_generic')
            }
            else {
                * Default to 4D if unclear
                replace CIIU_4D = `ciiu_generic' if missing(CIIU_4D) & !missing(`ciiu_generic')
            }
        }
        
        * Drop original CIIU to avoid conflicts in fuzzy search
        drop CIIU
    }
    
    * Search all variables for CIIU patterns (more flexible regex)
    local ciiu_vars_found ""
    foreach var of varlist * {
        * Skip if already processed
        if inlist("`var'", "CIIU_4D", "CIIU_3D", "CIIU_2D") continue
        
        * Check for CIIU/CIUU patterns (allow digits immediately after)
        * This catches CIIU2, CIIU3, CIIU4, CIUU2, etc.
        if regexm("`var'", "CII[UU]") {
            local ciiu_vars_found "`ciiu_vars_found' `var'"
            
            * Convert to string
            tempvar temp_ciiu
            capture confirm string variable `var'
            if _rc == 0 {
                gen `temp_ciiu' = `var'
            }
            else {
                tostring `var', gen(`temp_ciiu')
            }
            
            * Clean values
            replace `temp_ciiu' = trim(`temp_ciiu')
            replace `temp_ciiu' = "" if inlist(`temp_ciiu', ".", "0", " ")
            
            * Assign to appropriate level based on variable name patterns
            * Pattern 1: Explicit level indicator (4, N4, etc.)
            if regexm("`var'", "4") | regexm("`var'", "N4") {
                replace CIIU_4D = `temp_ciiu' if missing(CIIU_4D) & !missing(`temp_ciiu')
            }
            else if regexm("`var'", "3") | regexm("`var'", "N3") {
                replace CIIU_3D = `temp_ciiu' if missing(CIIU_3D) & !missing(`temp_ciiu')
            }
            * Pattern 2: CIIU2 or CIUU2 specifically (2-digit indicator)
            else if regexm("`var'", "CII[UU]2$") {
                replace CIIU_2D = `temp_ciiu' if missing(CIIU_2D) & !missing(`temp_ciiu')
            }
            * Pattern 3: Infer from data length
            else {
                quietly count if !missing(`temp_ciiu')
                if r(N) > 0 {
                    * Get typical length of this variable's data
                    gen temp_len = length(`temp_ciiu') if !missing(`temp_ciiu')
                    quietly summarize temp_len, detail
                    local data_len = r(p50)
                    drop temp_len
                    
                    if `data_len' >= 4 {
                        replace CIIU_4D = `temp_ciiu' if missing(CIIU_4D) & !missing(`temp_ciiu')
                    }
                    else if `data_len' == 3 {
                        replace CIIU_3D = `temp_ciiu' if missing(CIIU_3D) & !missing(`temp_ciiu')
                    }
                    else if `data_len' == 2 {
                        replace CIIU_2D = `temp_ciiu' if missing(CIIU_2D) & !missing(`temp_ciiu')
                    }
                    else {
                        replace CIIU_4D = `temp_ciiu' if missing(CIIU_4D) & !missing(`temp_ciiu')
                    }
                }
            }
        }
    }
    
    * Clean and standardize format
    foreach var in CIIU_4D CIIU_3D CIIU_2D {
        replace `var' = trim(`var')
        replace `var' = "" if inlist(`var', ".", "0", " ")
    }
    
    * Derive missing levels from available ones
    replace CIIU_3D = substr(CIIU_4D, 1, 3) if missing(CIIU_3D) & !missing(CIIU_4D) & length(CIIU_4D) >= 3
    replace CIIU_2D = substr(CIIU_4D, 1, 2) if missing(CIIU_2D) & !missing(CIIU_4D) & length(CIIU_4D) >= 2
    replace CIIU_2D = substr(CIIU_3D, 1, 2) if missing(CIIU_2D) & !missing(CIIU_3D) & length(CIIU_3D) >= 2
    
    * Store original values and source info
    gen ciiu_4d_original = CIIU_4D
    gen ciiu_3d_original = CIIU_3D  
    gen ciiu_2d_original = CIIU_2D
    gen ciiu_source = "`ciiu_vars_found'"
    
    * Verify CIIU capture success
    quietly count if !missing(CIIU_4D) | !missing(CIIU_3D) | !missing(CIIU_2D)
    if r(N) == 0 {
        display as error "WARNING: No CIIU captured for year `year'"
    }
    
    * Create harmonized CIIU (prefer 4D, fallback to 3D, then 2D)
    gen ciiu = CIIU_4D
    replace ciiu = CIIU_3D if missing(ciiu) & !missing(CIIU_3D)
    replace ciiu = CIIU_2D if missing(ciiu) & !missing(CIIU_2D)
    
    * Keep only common variables plus harmonized CIIU
    keep `common_vars' ciiu ciiu_4d_original ciiu_3d_original ciiu_2d_original ciiu_source
    
    * Add year identifier
    gen year = `year'
    
    * Append to panel
    if `year_count' == 0 {
        save `panel', replace
    }
    else {
        append using `panel', force
        save `panel', replace
    }
    
    local year_count = `year_count' + 1
}

use `panel', clear

*------------------------------------------------------------------------------*
* Create numeric CIIU variables
*------------------------------------------------------------------------------*

* Extract numeric components from harmonized CIIU
gen ciiu_2d = real(substr(ciiu, 1, 2))
gen ciiu_3d = real(substr(ciiu, 1, 3))

label variable ciiu "Harmonized industry code (CIIU 4-digit)"
label variable ciiu_2d "CIIU 2-digit (sector)"
label variable ciiu_3d "CIIU 3-digit (division)"

*------------------------------------------------------------------------------*
* Create unique firm identifier
*------------------------------------------------------------------------------*

* Generate firm ID from establishment and enterprise codes
* Assumption: NORDEMP (enterprise) + NORDEST (establishment) uniquely identify firms

capture confirm variable NORDEMP
capture confirm variable NORDEST

if _rc == 0 {
    egen firm_id = group(NORDEMP NORDEST)
}
else {
    display as error "WARNING: NORDEMP or NORDEST not found. Using alternative ID."
    
    * Fallback: use any available unique identifier
    ds
    local all_vars `r(varlist)'
    
    foreach var in CODIGO ID IDENTIF {
        if strpos("`all_vars'", "`var'") > 0 {
            egen firm_id = group(`var')
            continue, break
        }
    }
}

label variable firm_id "Unique establishment ID"

* Verify firm_id was created
capture confirm variable firm_id
if _rc != 0 {
    display as error "ERROR: Could not create firm_id"
    exit 198
}

* Sort panel
sort firm_id year

*------------------------------------------------------------------------------*
* Generate panel structure variables
*------------------------------------------------------------------------------*

* Identify first and last year for each firm
by firm_id: egen first_year = min(year)
by firm_id: egen last_year = max(year)
by firm_id: gen n_years_obs = _N

* Entry and exit indicators
by firm_id: gen entry = (_n == 1)
by firm_id: gen exit = (_n == _N)

* Adjust exit indicator (last observed year ≠ exit if it's final year in sample)
quietly: summarize year
local max_year = r(max)
replace exit = 0 if year == `max_year'

* Firm age (years since first appearance)
gen firm_age = year - first_year

* Balanced panel indicator
local min_year = `year_start'
gen balanced = (n_years_obs == `max_year' - `min_year' + 1)

* Label variables
label variable year "Year"
label variable first_year "First year in panel"
label variable last_year "Last year in panel"
label variable n_years_obs "Number of years observed"
label variable firm_age "Firm age (years since entry)"
label variable balanced "=1 if complete panel"
label variable exit "=1 if firm exits in this year"
label variable entry "=1 if firm enters in this year"

*------------------------------------------------------------------------------*
* Order variables and compress
*------------------------------------------------------------------------------*

order firm_id year NORDEMP NORDEST DPTO ciiu ciiu_2d ciiu_3d PERIODO ///
      first_year last_year n_years_obs firm_age balanced exit entry

compress

*------------------------------------------------------------------------------*
* Save panel
*------------------------------------------------------------------------------*

* Before saving we remove one duplicate
duplicates report firm_id year
duplicates tag firm_id year, gen(dup)
br if dup==1
duplicates drop firm_id year, force


save "$clean_dir/panel_eam_1992_2023.dta", replace

* Export summary statistics
preserve
collapse (count) n_firms=firm_id ///
         (sum) entries=entry exits=exit ///
         (mean) entry_rate=entry exit_rate=exit, ///
         by(year)
export delimited using "$clean_dir/panel_summary_by_year.csv", replace
restore

* Export balance distribution
preserve
collapse (count) n_firms=firm_id, by(n_years_obs)
gen pct = n_firms / sum(n_firms) * 100
gsort -n_years_obs
export delimited using "$clean_dir/panel_balance_distribution.csv", replace
restore

* Export data dictionary
preserve
describe, replace clear
export delimited using "$clean_dir/variables_dictionary.csv", replace
restore

*------------------------------------------------------------------------------*
* Summary statistics
*------------------------------------------------------------------------------*

xtset firm_id year
xtdescribe

quietly: count if balanced == 1
local n_balanced = r(N)

quietly: count if first_year <= 2001 & last_year >= 2001
local n_cross_wto = r(N)

summarize entry exit if year > `min_year' & year < `max_year'

log close
