/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 05_process_comtrade.do
PURPOSE: Process Comtrade trade data using harmonized concordances

AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
capture log close

global main_dir "C:\Users\dafrb\Desktop\EAM_data\CHINA-SCHOCK"
global raw_dir "$main_dir/raw_data"
global concordance_dir "$raw_dir/concordance"
global comtrade_dir "$raw_dir/comtrade"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"

foreach dir in clean_dir output_dir logs_dir {
    capture mkdir "${`dir'}"
}

log using "$logs_dir/05_comtrade_`c(current_date)'.log", replace text

*==============================================================================*
* PART 1: COLOMBIA IMPORTS (CHINA AND WORLD)
*==============================================================================*

* Load China imports
import delimited "$comtrade_dir/colombia_imports_from_china_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs6
rename primaryvalue imports_china

* Keep HS6 as string to preserve leading zeros
tostring hs6, replace format(%06.0f) force
destring year imports_china, replace force
keep year hs6 imports_china

collapse (sum) imports_china, by(hs6 year)

tempfile china_imports
save `china_imports'

* Load World imports
import delimited "$comtrade_dir/colombia_imports_from_world_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs6
rename primaryvalue imports_total

* Keep HS6 as string to preserve leading zeros
tostring hs6, replace format(%06.0f) force
destring year imports_total, replace force
keep year hs6 imports_total

collapse (sum) imports_total, by(hs6 year)

* Merge China and World at HS6-year level
merge 1:1 hs6 year using `china_imports', keep(match) nogen

* Apply concordance
merge m:1 hs6 using "$concordance_dir/hs6_isic4_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d)

* Collapse to CIIU-year level
collapse (sum) imports_china imports_total ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d year)

* Generate china_share
gen china_share = imports_china / imports_total

* Flag and cap anomalies (Comtrade data quality issue)
gen anomaly_china_gt_world = (china_share > 1)
replace china_share = 1 if china_share > 1

label var imports_china "Imports from China (USD)"
label var imports_total "Total imports from World (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU Rev 4 2-digit industry code"
label var china_share "China share of total imports (capped at 1)"
label var anomaly_china_gt_world "Flag: raw China imports > World"

sort ciiu_2d year
compress
save "$clean_dir/colombia_trade_ciiu.dta", replace

* Separate files for backward compatibility
preserve
    keep ciiu_2d year imports_china n_hs6_products
    save "$clean_dir/colombia_imports_china_ciiu.dta", replace
restore

preserve
    keep ciiu_2d year imports_total n_hs6_products
    save "$clean_dir/colombia_imports_total_ciiu.dta", replace
restore

*==============================================================================*
* PART 2: COLOMBIA EXPORTS
*==============================================================================*

import delimited "$comtrade_dir/colombia_exports_to_world_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs6
rename primaryvalue trade_value_usd

* Keep HS6 as string to preserve leading zeros
tostring hs6, replace format(%06.0f) force
destring year trade_value_usd, replace force
keep year hs6 trade_value_usd

merge m:1 hs6 using "$concordance_dir/hs6_isic4_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d)

collapse (sum) exports_total=trade_value_usd ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d year)

label var exports_total "Total exports to World (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU Rev 4 2-digit industry code"

sort ciiu_2d year
compress
save "$clean_dir/colombia_exports_total_ciiu.dta", replace

*==============================================================================*
* PART 3: CHINA EXPORTS TO LAC
*==============================================================================*

import delimited "$comtrade_dir/china_exports_to_lac_HS6.csv", clear varnames(1)

rename refyear year
rename cmdcode hs6
rename primaryvalue trade_value_usd

* Keep HS6 as string to preserve leading zeros
tostring hs6, replace format(%06.0f) force
destring year trade_value_usd, replace force
keep year hs6 trade_value_usd partner_country

merge m:1 hs6 using "$concordance_dir/hs6_isic4_concordance.dta", ///
      keep(match) nogen keepusing(ciiu_2d)

collapse (sum) exports_china=trade_value_usd ///
         (count) n_hs6_products=hs6, ///
         by(ciiu_2d partner_country year)

label var exports_china "China exports to LAC (USD)"
label var n_hs6_products "Number of HS6 products"
label var ciiu_2d "CIIU Rev 4 2-digit industry code"

sort ciiu_2d partner_country year
compress
save "$clean_dir/china_lac_exports_ciiu.dta", replace

*==============================================================================*
* PART 4: SUMMARY STATISTICS
*==============================================================================*

use "$clean_dir/colombia_trade_ciiu.dta", clear

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
    collapse (sum) imports_china imports_total, by(year)
    gen china_share = imports_china / imports_total
    export delimited using "$output_dir/colombia_trade_timeseries.csv", replace
restore

* LAC exports summary
use "$clean_dir/china_lac_exports_ciiu.dta", clear

preserve
    collapse (sum) total_exports=exports_china, by(ciiu_2d partner_country)
    reshape wide total_exports, i(ciiu_2d) j(partner_country) string
    export delimited using "$output_dir/lac_exports_by_ciiu_country.csv", replace
restore

log close
