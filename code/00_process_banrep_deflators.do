/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 00_process_banrep_deflators.do
PURPOSE: Process Banco de la República price indices to create deflators
         for productivity analysis following differentiated deflation approach

INPUTS:
    - graficador_series.xlsx (Banco de la República historical series)
    
OUTPUTS:
    - deflators_colombia_1992_2023.dta
    - Validation graphs (indices evolution and inflation rates)

METHODOLOGY:
    Differentiated deflation following Eslava et al. (2004):
    - Manufacturing output: IPP Manufacturing (domestic supply)
    - Intermediate inputs: IPP Intermediate consumption
    - Capital goods: IPP Capital goods
    - Labor costs: CPI Total
    
    Base year: 2018 (indices=100, deflators=1.0)

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
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"

foreach dir in main_dir raw_dir clean_dir logs_dir {
    capture mkdir "${`dir'}"
}

log using "$logs_dir/00_deflators_`c(current_date)'.log", replace text

*------------------------------------------------------------------------------*
* Import and identify price series
*------------------------------------------------------------------------------*

* Import Banrep Excel file (sheet "Datos" has 2 header rows)
local excel_file "$raw_dir/deflators/graficador_series.xlsx"
capture confirm file "`excel_file'"
if _rc {
    display as error "ERROR: Excel file not found at `excel_file'"
    exit 601
}

import excel "`excel_file'", sheet("Datos") firstrow clear

* Identify and rename key columns
* Column structure from Banrep:
*   J:  IPP Manufacturing (domestic supply) - end of month
*   Z:  IPP Intermediate consumption - end of month  
*   AH: IPP Capital goods - end of month
*   Inflacióntota~s: CPI Total - end of month

rename Fecha date_str
rename J ipp_manuf_raw
rename Z ipp_inter_raw
rename AH ipp_cap_raw
rename Inflacióntota~s ipc_raw

keep date_str ipp_manuf_raw ipp_inter_raw ipp_cap_raw ipc_raw

*------------------------------------------------------------------------------*
* Process dates and convert to numeric
*------------------------------------------------------------------------------*

* Remove units row (first row after headers)
drop if date_str == "dd/mm/aaaa"

* Parse dates (format: dd/mm/yyyy)
gen date_temp = date(date_str, "DMY")
format date_temp %td
gen year = year(date_temp)
gen month = month(date_temp)
drop date_temp date_str

* Convert to numeric (handle European format: comma as decimal separator)
foreach var in ipp_manuf_raw ipp_inter_raw ipp_cap_raw ipc_raw {
    replace `var' = "" if `var' == "-"
    replace `var' = subinstr(`var', ",", ".", .)
    
    local newvar = substr("`var'", 1, length("`var'") - 4)
    destring `var', gen(`newvar') force
    drop `var'
}

*------------------------------------------------------------------------------*
* Filter period and calculate annual averages
*------------------------------------------------------------------------------*

keep if year >= 1992 & year <= 2023
collapse (mean) ipp_manuf ipp_inter ipp_cap ipc, by(year)

*------------------------------------------------------------------------------*
* Create deflators with base year 2018
*------------------------------------------------------------------------------*

local base_year = 2018

* Get base year values
foreach var in ipp_manuf ipp_inter ipp_cap ipc {
    summarize `var' if year == `base_year', meanonly
    local base_`var' = r(mean)
}

* Generate indices (base 100 in 2018)
gen ipp_manuf_index = (ipp_manuf / `base_ipp_manuf') * 100
gen ipp_inter_index = (ipp_inter / `base_ipp_inter') * 100
gen ipp_cap_index = (ipp_cap / `base_ipp_cap') * 100
gen ipc_index = (ipc / `base_ipc') * 100

* Generate deflators (base 1.0 in 2018)
gen ipp_manuf_deflator = ipp_manuf / `base_ipp_manuf'
gen ipp_inter_deflator = ipp_inter / `base_ipp_inter'
gen ipp_cap_deflator = ipp_cap / `base_ipp_cap'
gen ipc_deflator = ipc / `base_ipc'

* Round for precision
foreach var of varlist *_index {
    replace `var' = round(`var', 0.01)
}
foreach var of varlist *_deflator {
    replace `var' = round(`var', 0.000001)
}

*------------------------------------------------------------------------------*
* Label variables
*------------------------------------------------------------------------------*

label variable year "Year"

label variable ipp_manuf_index "IPP Manufacturing index (base 2018=100)"
label variable ipp_inter_index "IPP Intermediate inputs index (base 2018=100)"
label variable ipp_cap_index "IPP Capital goods index (base 2018=100)"
label variable ipc_index "CPI Total index (base 2018=100)"

label variable ipp_manuf_deflator "IPP Manufacturing deflator (base 2018=1.0)"
label variable ipp_inter_deflator "IPP Intermediate inputs deflator (base 2018=1.0)"
label variable ipp_cap_deflator "IPP Capital goods deflator (base 2018=1.0)"
label variable ipc_deflator "CPI Total deflator (base 2018=1.0)"

label variable ipp_manuf "IPP Manufacturing (original Banrep value)"
label variable ipp_inter "IPP Intermediate inputs (original Banrep value)"
label variable ipp_cap "IPP Capital goods (original Banrep value)"
label variable ipc "CPI Total (original Banrep value)"

*------------------------------------------------------------------------------*
* Validation
*------------------------------------------------------------------------------*

* Verify base year values
assert abs(ipp_manuf_index - 100) < 0.01 if year == `base_year'
assert abs(ipp_inter_index - 100) < 0.01 if year == `base_year'
assert abs(ipp_cap_index - 100) < 0.01 if year == `base_year'
assert abs(ipc_index - 100) < 0.01 if year == `base_year'

assert abs(ipp_manuf_deflator - 1) < 0.000001 if year == `base_year'
assert abs(ipp_inter_deflator - 1) < 0.000001 if year == `base_year'
assert abs(ipp_cap_deflator - 1) < 0.000001 if year == `base_year'
assert abs(ipc_deflator - 1) < 0.000001 if year == `base_year'

* Check for missing values
misstable summarize ipp_manuf_deflator ipp_inter_deflator ipp_cap_deflator ipc_deflator

*------------------------------------------------------------------------------*
* Calculate inflation rates for reference
*------------------------------------------------------------------------------*

sort year
foreach var in ipp_manuf ipp_inter ipp_cap ipc {
    gen `var'_inflation = ((`var'_index/`var'_index[_n-1]) - 1) * 100 if year > 1992
    label variable `var'_inflation "`var' inflation rate (%)"
}

*------------------------------------------------------------------------------*
* Organize and save
*------------------------------------------------------------------------------*

order year ///
      ipp_manuf_index ipp_manuf_deflator ///
      ipp_inter_index ipp_inter_deflator ///
      ipp_cap_index ipp_cap_deflator ///
      ipc_index ipc_deflator ///
      ipp_manuf ipp_inter ipp_cap ipc ///
      ipp_manuf_inflation ipp_inter_inflation ipp_cap_inflation ipc_inflation

sort year
compress

notes: Deflators for productivity analysis - Colombia 1992-2023
notes: Source: Banco de la República historical series
notes: Base year: 2018 (indices = 100, deflators = 1.0)
notes: Methodology: Differentiated deflation (Eslava et al. 2004)
notes: Processed: `c(current_date)'

save "$clean_dir/deflators_colombia_1992_2023.dta", replace
export delimited using "$clean_dir/deflators_colombia_1992_2023.csv", replace

*------------------------------------------------------------------------------*
* Validation graphs
*------------------------------------------------------------------------------*

* Graph 1: Price indices evolution
twoway (line ipp_manuf_index year, lcolor(navy) lwidth(medium)) ///
       (line ipp_inter_index year, lcolor(maroon) lwidth(medium) lpattern(dash)) ///
       (line ipp_cap_index year, lcolor(forest_green) lwidth(medium) lpattern(shortdash)) ///
       (line ipc_index year, lcolor(orange) lwidth(medium) lpattern(dot)), ///
       title("Price Indices Evolution (1992-2023)", size(medium)) ///
       subtitle("Base 2018 = 100") ///
       xtitle("Year") ytitle("Index (2018=100)") ///
       xlabel(1992(4)2024, angle(45)) ///
       ylabel(, angle(0) format(%9.0f)) ///
       legend(order(1 "IPP Manufacturing" 2 "IPP Intermediates" ///
                    3 "IPP Capital" 4 "CPI Total") ///
              position(6) rows(2) size(small)) ///
       graphregion(color(white)) bgcolor(white) ///
       note("Source: Banco de la República", size(vsmall))

graph export "$clean_dir/indices_evolution.png", replace width(1200)

* Graph 2: Annual inflation rates
twoway (line ipp_manuf_inflation year, lcolor(navy) lwidth(medium)) ///
       (line ipp_inter_inflation year, lcolor(maroon) lwidth(medium)) ///
       (line ipp_cap_inflation year, lcolor(forest_green) lwidth(medium)) ///
       (line ipc_inflation year, lcolor(orange) lwidth(medium)), ///
       title("Annual Inflation by Price Index (1993-2023)", size(medium)) ///
       xtitle("Year") ytitle("Annual inflation (%)") ///
       xlabel(1992(4)2024, angle(45)) ///
       ylabel(, angle(0) format(%9.1f)) ///
       yline(0, lcolor(black) lwidth(thin)) ///
       xline(2001, lcolor(red) lpattern(dash) lwidth(thin)) ///
       legend(order(1 "IPP Manufacturing" 2 "IPP Intermediates" ///
                    3 "IPP Capital" 4 "CPI Total") ///
              position(6) rows(2) size(small)) ///
       graphregion(color(white)) bgcolor(white) ///
       note("Source: Banco de la República" ///
            "Vertical line: China WTO accession (2001)", size(vsmall))

graph export "$clean_dir/inflation_rates.png", replace width(1200)

*------------------------------------------------------------------------------*
* Summary statistics
*------------------------------------------------------------------------------*

summarize ipp_manuf_deflator ipp_inter_deflator ipp_cap_deflator ipc_deflator, ///
         detail format

* Period comparison
summarize *_inflation if year >= 1993 & year <= 2000
summarize *_inflation if year >= 2001

log close
