/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing  
FILE: 04_process_comtrade.do
PURPOSE: Complete Comtrade data processing and concordance construction

WORKFLOW:
1. Process Pierce & Schott HS→NAICS concordance
2. Process NAICS→ISIC concordance
3. Create master HS6→CIIU concordance
4. Apply to Colombia imports from China
5. Apply to Colombia total imports
6. Apply to Colombia exports
7. Apply to China exports to LAC
8. Validation and summary statistics

INPUTS:
    Raw data (Comtrade CSVs):
    - colombia_imports_from_china_HS6.csv
    - colombia_imports_from_world_HS6.csv
    - colombia_exports_to_world_HS6.csv
    - china_exports_to_lac_HS6.csv
    
    Concordance files:
    - hs_sic_naics_imports_89_123_20240801.dta (Pierce & Schott)
    - NAICS2012US-ISIC4.txt
    
OUTPUTS:
    Concordances:
    - hs6_ciiu2_concordance.dta (clean 1:1 mapping)
    - hs6_ciiu2_concordance_detailed.dta (with weights)
    
    Trade data by CIIU-year:
    - colombia_imports_china_ciiu.dta
    - colombia_imports_total_ciiu.dta
    - colombia_exports_total_ciiu.dta
    - china_lac_exports_ciiu.dta
    
AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
capture log close

*------------------------------------------------------------------------------*
* Setup
*------------------------------------------------------------------------------*

global main_dir "C:\Users\dafrb\Desktop\EAM_data"
global raw_dir "$main_dir/raw_data"
global concordance_dir "$raw_dir/concordance"
global comtrade_dir "$raw_dir/comtrade"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"

foreach dir in clean_dir output_dir logs_dir {
    capture mkdir "${`dir'}"
}

log using "$logs_dir/04_comtrade_`c(current_date)'.log", replace text

*==============================================================================*
* PART 1: BUILD HS6 → CIIU CONCORDANCE
*==============================================================================*

*------------------------------------------------------------------------------*
* Step 1.1: Process Pierce & Schott HS→NAICS
*------------------------------------------------------------------------------*

use "$concordance_dir/hs_sic_naics_imports_89_123_20240801.dta", clear

tostring commodity, gen(hs10_str) format(%010.0f) force
gen hs6_str = substr(hs10_str, 1, 6)
destring hs6_str, gen(hs6) force

keep hs6 naics year
drop if missing(hs6) | missing(naics) | hs6 == 0

bysort hs6 naics: gen n_obs = _N
bysort hs6: egen max_obs = max(n_obs)
keep if n_obs == max_obs

duplicates drop hs6 naics, force

preserve
    keep hs6 naics
    duplicates drop
    save "$concordance_dir/hs6_naics_temp.dta", replace
restore

*------------------------------------------------------------------------------*
* Step 1.2: Process NAICS→ISIC
*------------------------------------------------------------------------------*

import delimited "$concordance_dir/NAICS2012US-ISIC4.txt", clear varnames(1)

rename naics2012code naics
rename isic4code isic4
keep if naics2012part == 0
keep naics isic4
duplicates drop

destring naics, replace force
destring isic4, replace force
drop if missing(naics) | missing(isic4)

save "$concordance_dir/naics_isic_temp.dta", replace

*------------------------------------------------------------------------------*
* Step 1.3: Create master HS6→CIIU concordance
*------------------------------------------------------------------------------*

use "$concordance_dir/hs6_naics_temp.dta", clear

destring naics, replace force
merge m:1 naics using "$concordance_dir/naics_isic_temp.dta", keep(match master)

gen isic2 = floor(isic4/100) if !missing(isic4)
gen ciiu_2d = string(isic2, "%02.0f") if !missing(isic2)
keep if isic2 >= 10 & isic2 <= 39

bysort hs6: gen n_ciiu = _N
gen weight = 1/n_ciiu

keep hs6 ciiu_2d isic2 isic4 naics weight
order hs6 ciiu_2d isic2 weight

label var hs6 "HS 6-digit product code"
label var ciiu_2d "CIIU 2-digit industry code"
label var isic2 "ISIC 2-digit code"
label var isic4 "ISIC 4-digit code"
label var naics "NAICS 6-digit code"
label var weight "Weight for many-to-many matches"

compress
save "$concordance_dir/hs6_ciiu2_concordance_detailed.dta", replace

preserve
    bysort hs6: egen max_weight = max(weight)
    keep if weight == max_weight
    bysort hs6 (ciiu_2d): keep if _n == 1
    keep hs6 ciiu_2d
    rename ciiu_2d ciiu_2d_primary
    label var ciiu_2d_primary "Primary CIIU (most common match)"
    save "$concordance_dir/hs6_ciiu2_concordance.dta", replace
restore

*==============================================================================*
* PART 2: PROCESS TRADE DATA
*==============================================================================*

*------------------------------------------------------------------------------*
* Step 2.1: Colombia imports from China
*------------------------------------------------------------------------------*

import delimited "$comtrade_dir/colombia_imports_from_china_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs_code
rename primaryvalue trade_value_usd

destring year hs_code trade_value_usd, replace force
keep year hs_code trade_value_usd
rename hs_code hs6

merge m:1 hs6 using "$concordance_dir/hs6_ciiu2_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d_primary)

rename ciiu_2d_primary ciiu_2d

collapse (sum) imports_china=trade_value_usd ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d year)

label var imports_china "Imports from China (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU 2-digit industry code"

sort ciiu_2d year
compress
save "$clean_dir/colombia_imports_china_ciiu.dta", replace

*------------------------------------------------------------------------------*
* Step 2.2: Colombia total imports
*------------------------------------------------------------------------------*

import delimited "$comtrade_dir/colombia_imports_from_world_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs_code
rename primaryvalue trade_value_usd

destring year hs_code trade_value_usd, replace force
keep year hs_code trade_value_usd
rename hs_code hs6

merge m:1 hs6 using "$concordance_dir/hs6_ciiu2_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d_primary)

rename ciiu_2d_primary ciiu_2d

collapse (sum) imports_total=trade_value_usd ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d year)

label var imports_total "Total imports from World (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU 2-digit industry code"

sort ciiu_2d year
compress
save "$clean_dir/colombia_imports_total_ciiu.dta", replace

*------------------------------------------------------------------------------*
* Step 2.3: Colombia exports
*------------------------------------------------------------------------------*

import delimited "$comtrade_dir/colombia_exports_to_world_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs_code
rename primaryvalue trade_value_usd

destring year hs_code trade_value_usd, replace force
keep year hs_code trade_value_usd
rename hs_code hs6

merge m:1 hs6 using "$concordance_dir/hs6_ciiu2_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d_primary)

rename ciiu_2d_primary ciiu_2d

collapse (sum) exports_total=trade_value_usd ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d year)

label var exports_total "Total exports to World (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU 2-digit industry code"

sort ciiu_2d year
compress
save "$clean_dir/colombia_exports_total_ciiu.dta", replace

*------------------------------------------------------------------------------*
* Step 2.4: China exports to LAC
*------------------------------------------------------------------------------*

import delimited "$comtrade_dir/china_exports_to_lac_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs_code
rename primaryvalue trade_value_usd

destring year hs_code trade_value_usd, replace force
keep year hs_code trade_value_usd partner_country
rename hs_code hs6

merge m:1 hs6 using "$concordance_dir/hs6_ciiu2_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d_primary)

rename ciiu_2d_primary ciiu_2d

collapse (sum) exports_china=trade_value_usd ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d partner_country year)

label var exports_china "China exports to LAC (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU 2-digit industry code"

sort ciiu_2d partner_country year
compress
save "$clean_dir/china_lac_exports_ciiu.dta", replace

*==============================================================================*
* PART 3: SUMMARY STATISTICS AND VALIDATION
*==============================================================================*

*------------------------------------------------------------------------------*
* Calculate China import share
*------------------------------------------------------------------------------*

use "$clean_dir/colombia_imports_china_ciiu.dta", clear
merge 1:1 ciiu_2d year using "$clean_dir/colombia_imports_total_ciiu.dta", ///
      keep(match) nogen

gen china_share = imports_china / imports_total
label var china_share "China share of total imports"

compress
save "$clean_dir/colombia_trade_ciiu.dta", replace

*------------------------------------------------------------------------------*
* Summary statistics
*------------------------------------------------------------------------------*

* Top industries by China imports
preserve
    collapse (sum) total_imports=imports_china, by(ciiu_2d)
    gsort -total_imports
    gen rank = _n
    export delimited using "$output_dir/top_industries_china_imports.csv" ///
           if rank <= 10, replace
restore

* China share by industry
preserve
    keep if year >= 2000
    collapse (mean) avg_china_share=china_share, by(ciiu_2d)
    gsort -avg_china_share
    export delimited using "$output_dir/china_share_by_industry.csv", replace
restore

* Time series
preserve
    collapse (sum) imports_china imports_total ///
             (mean) china_share, by(year)
    export delimited using "$output_dir/colombia_trade_timeseries.csv", replace
restore

*------------------------------------------------------------------------------*
* LAC exports summary
*------------------------------------------------------------------------------*

use "$clean_dir/china_lac_exports_ciiu.dta", clear

preserve
    collapse (sum) total_exports=exports_china, by(ciiu_2d partner_country)
    reshape wide total_exports, i(ciiu_2d) j(partner_country) string
    export delimited using "$output_dir/lac_exports_by_ciiu_country.csv", replace
restore

log close
