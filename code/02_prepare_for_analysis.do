/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 02_prepare_for_analysis.do
PURPOSE: Prepare EAM panel for productivity analysis by deflating variables,
         conducting quality checks, and creating necessary transformations

INPUTS:
    - panel_eam_1992_2023.dta (from script 01)
    - deflators_colombia_1992_2023.dta (from script 00)
    
OUTPUTS:
    - panel_eam_clean.dta (ready for TFP estimation)
    - data_quality_report.csv (summary of checks and filters)
    - outliers_removed.csv (list of extreme observations)

METHODOLOGY:
    Differentiated deflation following Eslava et al. (2004):
    - Output/Sales: IPP Manufacturing (domestic supply)
    - Intermediate inputs: IPP Intermediate consumption  
    - Capital: IPP Capital goods
    - Labor costs: CPI Total
    
    Quality filters:
    - Remove missing values in key production variables
    - Remove zeros and negatives in Y, K, L, M
    - Winsorize at 1st and 99th percentiles by industry-year
    - Verify accounting identities (M < Y, VA > 0)

AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
set maxvar 10000
capture log close

*------------------------------------------------------------------------------*
* Setup
*------------------------------------------------------------------------------*

global main_dir "C:\Users\dafrb\Desktop\EAM_data\CHINA-SCHOCK"
global raw_dir  "$main_dir/raw_data"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"

foreach dir in main_dir raw_dir clean_dir output_dir logs_dir {
    capture mkdir "${`dir'}"
}

log using "$logs_dir/02_prepare_`c(current_date)'.log", replace text

*------------------------------------------------------------------------------*
* Load panel and merge with deflators
*------------------------------------------------------------------------------*

use "$clean_dir/panel_eam_1992_2023.dta", clear

local n_obs_initial = _N
distinct firm_id
local n_firms_initial = r(ndistinct)

* Merge with deflators
merge m:1 year using "$clean_dir/deflators_colombia_1992_2023.dta", ///
    keepusing(ipp_manuf_deflator ipp_inter_deflator ipp_cap_deflator ipc_deflator)

assert _merge == 3
drop _merge

*------------------------------------------------------------------------------*
* Define key economic variables from EAM
*------------------------------------------------------------------------------*

* Output: Production value (gross output)
* Use PRODBR2 (producción bruta) as primary measure
gen output_nominal = PRODBR2
label variable output_nominal "Gross output (nominal pesos)"

* Alternative: Sales (VALORVEN) if production missing
replace output_nominal = VALORVEN if missing(output_nominal) & !missing(VALORVEN)

* Capital: Fixed assets stock
gen capital_nominal = ACTIVFI
label variable capital_nominal "Fixed assets stock (nominal pesos)"

* Labor: Average employment (permanent + temporary)
* PPERYTEM = Promedio personal permanente y temporal
gen labor = PPERYTEM
label variable labor "Average employment (permanent + temporary)"

* Alternative: Total personnel if PPERYTEM missing
replace labor = PERTOTAL if missing(labor) & !missing(PERTOTAL)

* Intermediate inputs: Materials and energy
gen intermediates_nominal = CONSIN2
label variable intermediates_nominal "Intermediate inputs (nominal pesos)"

* Labor costs: Salaries and benefits
gen labor_cost_nominal = SALPEYTE
label variable labor_cost_nominal "Total labor costs (nominal pesos)"

* Alternative: Only permanent workers if total missing
replace labor_cost_nominal = SALARPER + PRESSPER if missing(labor_cost_nominal) ///
    & !missing(SALARPER) & !missing(PRESSPER)

*------------------------------------------------------------------------------*
* Deflate monetary variables using differentiated deflators
*------------------------------------------------------------------------------*

* Output and sales: IPP Manufacturing
gen output_real = output_nominal / ipp_manuf_deflator
label variable output_real "Gross output (real, pesos 2018)"

* Intermediate inputs: IPP Intermediate consumption
gen intermediates_real = intermediates_nominal / ipp_inter_deflator
label variable intermediates_real "Intermediate inputs (real, pesos 2018)"

* Capital: IPP Capital goods
gen capital_real = capital_nominal / ipp_cap_deflator
label variable capital_real "Capital stock (real, pesos 2018)"

* Labor costs: CPI Total
gen labor_cost_real = labor_cost_nominal / ipc_deflator
label variable labor_cost_real "Labor costs (real, pesos 2018)"

* Value added (real)
gen va_real = output_real - intermediates_real
label variable va_real "Value added (real, pesos 2018)"

*------------------------------------------------------------------------------*
* Quality checks - Missing values
*------------------------------------------------------------------------------*

* Count missing observations in key variables
local key_vars "output_real capital_real labor intermediates_real"

* Store missing counts in locals first
local i = 1
foreach var of local key_vars {
    quietly count if missing(`var')
    local miss_`i' = r(N)
    local miss_pct_`i' = (r(N) / `n_obs_initial') * 100
    local i = `i' + 1
}

* Create report dataset
preserve
clear
set obs 4
gen str30 variable = ""
gen missing_count = .
gen missing_pct = .

local i = 1
foreach var of local key_vars {
    replace variable = "`var'" in `i'
    replace missing_count = `miss_`i'' in `i'
    replace missing_pct = `miss_pct_`i'' in `i'
    local i = `i' + 1
}

list, clean noobs
export delimited using "$output_dir/missing_values_report.csv", replace
restore

* Drop observations with missing key production variables
gen missing_any = missing(output_real) | missing(capital_real) | ///
                  missing(labor) | missing(intermediates_real)

quietly count if missing_any == 1
local n_missing = r(N)

drop if missing_any == 1
drop missing_any

*------------------------------------------------------------------------------*
* Quality checks - Zeros and negatives
*------------------------------------------------------------------------------*

* Identify problematic observations
gen zero_negative = (output_real <= 0) | (capital_real <= 0) | ///
                    (labor <= 0) | (intermediates_real <= 0)

quietly count if zero_negative == 1
local n_zero_neg = r(N)

drop if zero_negative == 1
drop zero_negative

*------------------------------------------------------------------------------*
* Quality checks - Accounting identities
*------------------------------------------------------------------------------*

* Intermediate inputs should not exceed gross output
gen m_exceeds_y = (intermediates_real > output_real)
quietly count if m_exceeds_y == 1
local n_m_exceeds = r(N)
drop if m_exceeds_y == 1
drop m_exceeds_y

* Value added should be positive
gen va_negative = (va_real <= 0)
quietly count if va_negative == 1
local n_va_neg = r(N)
drop if va_negative == 1
drop va_negative

*------------------------------------------------------------------------------*
* Quality checks - Extreme outliers
*------------------------------------------------------------------------------*

* Winsorize at 1st and 99th percentiles by industry-year
* Use CIIU 2-digit as industry definition

foreach var in output_real capital_real labor intermediates_real va_real {
    * Calculate percentiles by industry-year
    egen p1_`var' = pctile(`var'), p(1) by(ciiu_2d year)
    egen p99_`var' = pctile(`var'), p(99) by(ciiu_2d year)
    
    * Flag outliers
    gen outlier_`var' = (`var' < p1_`var') | (`var' > p99_`var')
    
    * Winsorize
    replace `var' = p1_`var' if `var' < p1_`var' & !missing(`var')
    replace `var' = p99_`var' if `var' > p99_`var' & !missing(`var')
    
    drop p1_`var' p99_`var'
}

* Identify observations with any outlier
egen any_outlier = rowtotal(outlier_*)
replace any_outlier = (any_outlier > 0)

quietly count if any_outlier == 1
local n_outliers = r(N)

* Keep outlier flags for reference but don't drop
* (winsorization already applied)
drop outlier_* any_outlier

*------------------------------------------------------------------------------*
* Create log transformations for production function estimation
*------------------------------------------------------------------------------*

gen ln_output = ln(output_real)
gen ln_capital = ln(capital_real)
gen ln_labor = ln(labor)
gen ln_intermediates = ln(intermediates_real)
gen ln_va = ln(va_real)

label variable ln_output "Log gross output (real)"
label variable ln_capital "Log capital stock (real)"
label variable ln_labor "Log employment"
label variable ln_intermediates "Log intermediate inputs (real)"
label variable ln_va "Log value added (real)"

*------------------------------------------------------------------------------*
* Create lagged variables for productivity estimation
*------------------------------------------------------------------------------*

sort firm_id year
xtset firm_id year

* Lags needed for LP/ACF/GNR methods
gen ln_capital_lag = L.ln_capital
gen ln_labor_lag = L.ln_labor
gen ln_intermediates_lag = L.ln_intermediates

label variable ln_capital_lag "Log capital (t-1)"
label variable ln_labor_lag "Log labor (t-1)"
label variable ln_intermediates_lag "Log intermediates (t-1)"

*------------------------------------------------------------------------------*
* Create additional control variables
*------------------------------------------------------------------------------*

* Average wage (proxy for skill level)
gen avg_wage = labor_cost_real / labor if labor > 0
gen ln_avg_wage = ln(avg_wage)
label variable avg_wage "Average wage (real, pesos 2018)"
label variable ln_avg_wage "Log average wage"

* Capital-labor ratio (capital intensity)
gen capital_labor = capital_real / labor if labor > 0
gen ln_capital_labor = ln(capital_labor)
label variable capital_labor "Capital-labor ratio"
label variable ln_capital_labor "Log capital-labor ratio"

* Labor productivity
gen labor_prod = output_real / labor if labor > 0
gen ln_labor_prod = ln(labor_prod)
label variable labor_prod "Labor productivity (output per worker)"
label variable ln_labor_prod "Log labor productivity"

* Value added per worker
gen va_per_worker = va_real / labor if labor > 0
gen ln_va_per_worker = ln(va_per_worker)
label variable va_per_worker "Value added per worker"
label variable ln_va_per_worker "Log value added per worker"

*------------------------------------------------------------------------------*
* Flag observations valid for TFP estimation
*------------------------------------------------------------------------------*

* Observations must have:
* 1. Non-missing production variables
* 2. At least one lag (for dynamic methods)
* 3. Valid CIIU classification

gen valid_for_tfp = !missing(ln_output) & !missing(ln_capital) & ///
                    !missing(ln_labor) & !missing(ln_intermediates) & ///
                    !missing(ln_capital_lag) & ///
                    !missing(ciiu_2d)

label variable valid_for_tfp "=1 if observation can be used for TFP estimation"

quietly count if valid_for_tfp == 1
local n_valid_tfp = r(N)

*------------------------------------------------------------------------------*
* Industry-level statistics for TFP estimation feasibility
*------------------------------------------------------------------------------*

* Count firms and observations by industry
preserve
keep if valid_for_tfp == 1
collapse (count) n_obs=firm_id (sum) n_firms=entry, by(ciiu_2d)
gsort -n_obs

* Minimum threshold for TFP estimation: 100 observations
gen tfp_feasible = (n_obs >= 100)

list ciiu_2d n_obs n_firms tfp_feasible in 1/20, clean noobs
export delimited using "$output_dir/industry_sample_sizes.csv", replace
restore

*------------------------------------------------------------------------------*
* Create year indicators for China shock analysis
*------------------------------------------------------------------------------*

gen pre_wto = (year < 2001)
gen post_wto = (year >= 2001)
gen wto_year = (year == 2001)

label variable pre_wto "=1 if year before China WTO accession"
label variable post_wto "=1 if year after China WTO accession (2001+)"
label variable wto_year "=1 if year 2001 (China WTO accession)"

* Time periods for analysis
gen period = .
replace period = 1 if year >= 1992 & year <= 2000  // Pre-WTO
replace period = 2 if year >= 2001 & year <= 2010  // Early post-WTO
replace period = 3 if year >= 2011 & year <= 2023  // Late post-WTO

label define period_lbl 1 "1992-2000: Pre-WTO" 2 "2001-2010: Early post-WTO" ///
                        3 "2011-2023: Late post-WTO"
label values period period_lbl
label variable period "Analysis period"

*------------------------------------------------------------------------------*
* Organize variables and compress
*------------------------------------------------------------------------------*

order firm_id year ciiu ciiu_2d ciiu_3d ///
      output_real capital_real labor intermediates_real va_real ///
      ln_output ln_capital ln_labor ln_intermediates ln_va ///
      ln_capital_lag ln_labor_lag ln_intermediates_lag ///
      labor_prod va_per_worker avg_wage capital_labor ///
      valid_for_tfp pre_wto post_wto period ///
      first_year last_year firm_age entry exit balanced

compress

*------------------------------------------------------------------------------*
* Save cleaned panel
*------------------------------------------------------------------------------*

notes: Panel prepared for productivity analysis
notes: Base year for deflation: 2018
notes: Differentiated deflation applied (Eslava et al. 2004)
notes: Outliers winsorized at 1st and 99th percentiles by industry-year
notes: Processed: `c(current_date)'

save "$clean_dir/panel_eam_clean.dta", replace

*------------------------------------------------------------------------------*
* Generate data quality report
*------------------------------------------------------------------------------*

* Calculate final statistics before creating report dataset
quietly: use "$clean_dir/panel_eam_clean.dta", clear
local n_obs_final = _N
quietly: distinct firm_id
local n_firms_final = r(ndistinct)

* Now create report dataset
preserve
clear
set obs 10

gen str40 check = ""
gen value = .

replace check = "Initial observations" in 1
replace value = `n_obs_initial' in 1

replace check = "Dropped: missing key vars" in 2
replace value = `n_missing' in 2

replace check = "Dropped: zeros/negatives" in 3
replace value = `n_zero_neg' in 3

replace check = "Dropped: M > Y" in 4
replace value = `n_m_exceeds' in 4

replace check = "Dropped: VA <= 0" in 5
replace value = `n_va_neg' in 5

replace check = "Winsorized (outliers)" in 6
replace value = `n_outliers' in 6

replace check = "Final observations" in 7
replace value = `n_obs_final' in 7

replace check = "Final firms" in 8
replace value = `n_firms_final' in 8

replace check = "Valid for TFP estimation" in 9
replace value = `n_valid_tfp' in 9

gen pct_retained = (value / `n_obs_initial') * 100 if inlist(_n, 7, 9)

list, clean noobs separator(6)
export delimited using "$output_dir/data_quality_report.csv", replace
restore

*------------------------------------------------------------------------------*
* Summary statistics by period
*------------------------------------------------------------------------------*

use "$clean_dir/panel_eam_clean.dta", clear

preserve
collapse (mean) mean_output=output_real mean_capital=capital_real ///
              mean_labor=labor mean_va=va_real ///
              mean_labor_prod=labor_prod ///
         (median) med_output=output_real med_labor=labor ///
         (sd) sd_output=output_real sd_labor=labor ///
         (count) n_obs=firm_id, ///
         by(period)

list, clean noobs
export delimited using "$output_dir/summary_by_period.csv", replace
restore

*------------------------------------------------------------------------------*
* Final verification
*------------------------------------------------------------------------------*

* Verify xtset works
xtset firm_id year
xtdescribe

* Check for duplicates
duplicates report firm_id year
assert r(N) == r(unique_value)

* Verify key variables non-missing for valid_for_tfp observations
assert !missing(ln_output) if valid_for_tfp == 1
assert !missing(ln_capital) if valid_for_tfp == 1
assert !missing(ln_labor) if valid_for_tfp == 1
assert !missing(ln_intermediates) if valid_for_tfp == 1
assert !missing(ciiu_2d) if valid_for_tfp == 1

log close
