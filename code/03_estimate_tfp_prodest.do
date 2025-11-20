/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: 03_estimate_tfp_prodest.do
PURPOSE: Estimate Total Factor Productivity using multiple methods via prodest
         
METHODS IMPLEMENTED:
    1. Levinsohn-Petrin (2003) - LP
    2. Wooldridge (2009) - WRDG  
    3. Ackerberg-Caves-Frazer (2015) - ACF
    4. Gandhi-Navarro-Rivers (2020) - GNR (if separately estimated)

INPUTS:
    - panel_eam_clean.dta (from script 02)
    
OUTPUTS:
    - panel_eam_with_tfp.dta (panel with TFP estimates from all methods)
    - tfp_coefficients_by_method.csv (production function coefficients)
    - tfp_comparison_stats.csv (comparison across methods)
    - tfp_correlations.csv (correlations between methods)

METHODOLOGY:
    All methods estimated using prodest package (Mollisi & Rovigatti, 2017)
    
    Key advantages of prodest:
    - Unified interface for multiple estimators
    - GMM optimization instead of NLS
    - Robust standard errors
    - ACF correction available
    - Wooldridge one-step GMM implementation
    
    Industry level: CIIU 2-digit
    Minimum threshold: 100 observations per industry

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
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"

foreach dir in clean_dir output_dir logs_dir {
    capture mkdir "${`dir'}"
}

*ssc install prodest

log using "$logs_dir/03_tfp_prodest_`c(current_date)'.log", replace text

*------------------------------------------------------------------------------*
* Load cleaned panel
*------------------------------------------------------------------------------*

use "$clean_dir/panel_eam_clean.dta", clear

* Keep only observations valid for TFP estimation
keep if valid_for_tfp == 1

local n_obs_total = _N

* Set panel structure
xtset firm_id year

*------------------------------------------------------------------------------*
* Identify industries with sufficient observations
*------------------------------------------------------------------------------*

* Count observations by industry
egen n_obs_industry = count(firm_id), by(ciiu_2d)
egen n_firms_industry = count(firm_id), by(ciiu_2d year)
egen n_periods_industry = count(firm_id), by(ciiu_2d firm_id)

* Summary by industry
preserve
    bysort ciiu_2d firm_id: gen firm_tag = (_n == 1)
    collapse (count) n_obs=firm_id ///
             (mean) avg_firms=n_firms_industry ///
             (sum) n_distinct_firms=firm_tag, ///
             by(ciiu_2d)
    
    * Label for interpretation
    label variable ciiu_2d "CIIU 2-digit code"
    label variable n_obs "Total observations"
    label variable avg_firms "Average firms per year"
    label variable n_distinct_firms "Distinct firms"
    
    list ciiu_2d n_obs avg_firms n_distinct_firms, separator(0) abbreviate(20)
    
    * Export
    export delimited using "$output_dir/tfp_industry_summary.csv", replace
restore

* Keep industries with at least 100 observations
keep if n_obs_industry >= 100

* Get list of industries
levelsof ciiu_2d, local(industries) clean
local n_industries : word count `industries'

*------------------------------------------------------------------------------*
* Initialize TFP variables for multiple methods
*------------------------------------------------------------------------------*

* Levinsohn-Petrin
gen omega_lp = .
label variable omega_lp "TFP (Levinsohn-Petrin 2003)"

* Wooldridge  
gen omega_wrdg = .
label variable omega_wrdg "TFP (Wooldridge 2009)"

* Ackerberg-Caves-Frazer
gen omega_acf = .
label variable omega_acf "TFP (Ackerberg-Caves-Frazer 2015)"

*------------------------------------------------------------------------------*
* Storage for production function coefficients
*------------------------------------------------------------------------------*

tempfile coef_storage
preserve
    clear
    
    * Initialize dataset to store coefficients
    gen str10 method = ""
    gen ciiu_2d = .
    gen alpha_l = .
    gen alpha_k = .
    gen alpha_m = .
    gen se_l = .
    gen se_k = .
    gen se_m = .
    gen n_obs = .
    gen n_firms = .
    gen converged = 0
    
    * Labels
    label variable method "Estimation method"
    label variable ciiu_2d "CIIU 2-digit code"
    label variable alpha_l "Labor coefficient"
    label variable alpha_k "Capital coefficient"
    label variable alpha_m "Intermediates coefficient"
    label variable se_l "SE(Labor)"
    label variable se_k "SE(Capital)"
    label variable se_m "SE(Intermediates)"
    label variable n_obs "Observations"
    label variable n_firms "Number of firms"
    label variable converged "Convergence indicator"
    
    save `coef_storage', replace
restore

*------------------------------------------------------------------------------*
* Estimate TFP by industry using multiple methods
*------------------------------------------------------------------------------*

* Loop over industries
local counter = 1
foreach ind of local industries {
    
    preserve
    keep if ciiu_2d == `ind'
    
    local n_obs_ind = _N
    quietly: distinct firm_id
    local n_firms_ind = r(ndistinct)
    
    * Verify we have variation
    quietly: summarize ln_output
    if r(sd) < 0.01 | r(N) < 100 {
        restore
        local counter = `counter' + 1
        continue
    }
    
    * Initialize convergence flags
    local lp_converged = 0
    local wrdg_converged = 0
    local acf_converged = 0
    
    *--------------------------------------------------------------------------*
    * METHOD 1: Levinsohn-Petrin (2003)
    *--------------------------------------------------------------------------*    
    capture {
        prodest ln_output, ///
            free(ln_labor) ///
            state(ln_capital) ///
            proxy(ln_intermediates) ///
            method(lp) ///
            poly(3) ///
            va ///
            opt(bfgs) ///
            maxiter(500) ///
            id(firm_id) ///
            t(year)
        
        * Check convergence
        if _rc == 0 {
            local lp_converged = 1
            
            * Store coefficients
            matrix b_lp = e(b)
            local alpha_l_lp = b_lp[1,1]
            local alpha_k_lp = b_lp[1,2]
            local alpha_m_lp = b_lp[1,3]
            
            * Standard errors
            matrix V_lp = e(V)
            local se_l_lp = sqrt(V_lp[1,1])
            local se_k_lp = sqrt(V_lp[2,2])
            local se_m_lp = sqrt(V_lp[3,3])
            
            * Get TFP predictions
            predict double omega_lp_temp, residuals
            
            * Store in main dataset
            tempfile temp_lp
            keep firm_id year omega_lp_temp
            save `temp_lp'
        }
    }
    
    *--------------------------------------------------------------------------*
    * METHOD 2: Wooldridge (2009)
    *--------------------------------------------------------------------------*
    *--------------------------------------------------------------------------*
    * METHOD 2: Wooldridge (2009)
    *--------------------------------------------------------------------------*
    restore
    preserve
    keep if ciiu_2d == `ind'
    
    capture {
        prodest ln_output, ///
            free(ln_labor) ///
            state(ln_capital) ///
            proxy(ln_intermediates) ///
            method(wrdg) ///
            poly(3) ///
            va ///
            opt(bfgs) ///
            maxiter(500) ///
            id(firm_id) ///
            t(year)
        
        * Check convergence
        if _rc == 0 {
            local wrdg_converged = 1
            
            * Store coefficients
            matrix b_wrdg = e(b)
            local alpha_l_wrdg = b_wrdg[1,1]
            local alpha_k_wrdg = b_wrdg[1,2]
            local alpha_m_wrdg = b_wrdg[1,3]
            
            * Standard errors
            matrix V_wrdg = e(V)
            local se_l_wrdg = sqrt(V_wrdg[1,1])
            local se_k_wrdg = sqrt(V_wrdg[2,2])
            local se_m_wrdg = sqrt(V_wrdg[3,3])
            
            * Get TFP predictions
            predict double omega_wrdg_temp, residuals
            
            * Store in tempfile
            tempfile temp_wrdg
            keep firm_id year omega_wrdg_temp
            save `temp_wrdg', replace
        }
    }
    
    *--------------------------------------------------------------------------*
    * METHOD 3: Ackerberg-Caves-Frazer (2015)
    *--------------------------------------------------------------------------*
    restore
    preserve
    keep if ciiu_2d == `ind'
    
	capture {
        prodest ln_output, ///
            free(ln_labor) ///
            state(ln_capital) ///
            proxy(ln_intermediates) ///
            method(lp) ///
            acf ///
            poly(3) ///
            va ///
            opt(bfgs) ///
            maxiter(500) ///
            id(firm_id) ///
            t(year)
        
        * Check convergence
        if _rc == 0 {
            local acf_converged = 1
            
            * Store coefficients
            matrix b_acf = e(b)
            local alpha_l_acf = b_acf[1,1]
            local alpha_k_acf = b_acf[1,2]
            local alpha_m_acf = b_acf[1,3]
            
            * Standard errors
            matrix V_acf = e(V)
            local se_l_acf = sqrt(V_acf[1,1])
            local se_k_acf = sqrt(V_acf[2,2])
            local se_m_acf = sqrt(V_acf[3,3])
            
            * Get TFP predictions
            predict double omega_acf_temp, residuals
            
            * Store in main dataset
            tempfile temp_acf
            keep firm_id year omega_acf_temp
            save `temp_acf'
        }
    }
    
    *--------------------------------------------------------------------------*
    * Store coefficients
    *--------------------------------------------------------------------------*
    
    restore
    
    * Merge LP results
    if `lp_converged' == 1 {
        merge 1:1 firm_id year using `temp_lp', ///
            update replace ///
            keep(master match) ///
            nogenerate
        replace omega_lp = omega_lp_temp if ciiu_2d == `ind'
        drop omega_lp_temp
        
        * Store coefficients
        preserve
            use `coef_storage', clear
            local new = _N + 1
            set obs `new'
            replace method = "LP" in `new'
            replace ciiu_2d = `ind' in `new'
            replace alpha_l = `alpha_l_lp' in `new'
            replace alpha_k = `alpha_k_lp' in `new'
            replace alpha_m = `alpha_m_lp' in `new'
            replace se_l = `se_l_lp' in `new'
            replace se_k = `se_k_lp' in `new'
            replace se_m = `se_m_lp' in `new'
            replace n_obs = `n_obs_ind' in `new'
            replace n_firms = `n_firms_ind' in `new'
            replace converged = 1 in `new'
            save `coef_storage', replace
        restore
    }
    
    * Merge Wooldridge results
    if `wrdg_converged' == 1 {
        merge 1:1 firm_id year using `temp_wrdg', ///
            update replace ///
            keep(master match) ///
            nogenerate
        replace omega_wrdg = omega_wrdg_temp if ciiu_2d == `ind'
        drop omega_wrdg_temp
        
        * Store coefficients
        preserve
            use `coef_storage', clear
            local new = _N + 1
            set obs `new'
            replace method = "WRDG" in `new'
            replace ciiu_2d = `ind' in `new'
            replace alpha_l = `alpha_l_wrdg' in `new'
            replace alpha_k = `alpha_k_wrdg' in `new'
            replace alpha_m = `alpha_m_wrdg' in `new'
            replace se_l = `se_l_wrdg' in `new'
            replace se_k = `se_k_wrdg' in `new'
            replace se_m = `se_m_wrdg' in `new'
            replace n_obs = `n_obs_ind' in `new'
            replace n_firms = `n_firms_ind' in `new'
            replace converged = 1 in `new'
            save `coef_storage', replace
        restore
    }
    
    * Merge ACF results
    if `acf_converged' == 1 {
        merge 1:1 firm_id year using `temp_acf', ///
            update replace ///
            keep(master match) ///
            nogenerate
        replace omega_acf = omega_acf_temp if ciiu_2d == `ind'
        drop omega_acf_temp
        
        * Store coefficients
        preserve
            use `coef_storage', clear
            local new = _N + 1
            set obs `new'
            replace method = "ACF" in `new'
            replace ciiu_2d = `ind' in `new'
            replace alpha_l = `alpha_l_acf' in `new'
            replace alpha_k = `alpha_k_acf' in `new'
            replace alpha_m = `alpha_m_acf' in `new'
            replace se_l = `se_l_acf' in `new'
            replace se_k = `se_k_acf' in `new'
            replace se_m = `se_m_acf' in `new'
            replace n_obs = `n_obs_ind' in `new'
            replace n_firms = `n_firms_ind' in `new'
            replace converged = 1 in `new'
            save `coef_storage', replace
        restore
    }
    
    local counter = `counter' + 1
}

*------------------------------------------------------------------------------*
* Export coefficients
*------------------------------------------------------------------------------*

preserve
    use `coef_storage', clear
    drop if method == ""
    
    * Sort by industry and method
    sort ciiu_2d method

    * Export
    export delimited using "$output_dir/tfp_coefficients_by_method.csv", replace
restore

*------------------------------------------------------------------------------*
* TFP correlations
*------------------------------------------------------------------------------*

* Correlations between methods
correlate omega_lp omega_wrdg omega_acf if !missing(omega_lp, omega_wrdg, omega_acf)

* Export correlation matrix
preserve
    correlate omega_lp omega_wrdg omega_acf if !missing(omega_lp, omega_wrdg, omega_acf)
    matrix C = r(C)
    
    clear
    svmat C, names(col)
    
    gen method = ""
    replace method = "LP" in 1
    replace method = "WRDG" in 2
    replace method = "ACF" in 3
    
    order method
    
    export delimited using "$output_dir/tfp_correlations.csv", replace
restore
*------------------------------------------------------------------------------*
* Save panel with TFP estimates
*------------------------------------------------------------------------------*

* Label all TFP variables
label variable omega_lp "Log TFP (Levinsohn-Petrin)"
label variable omega_wrdg "Log TFP (Wooldridge)"
label variable omega_acf "Log TFP (Ackerberg-Caves-Frazer)"

* Sort
sort firm_id year

* Save
save "$clean_dir/panel_eam_with_tfp.dta", replace

local n_obs_final = _N

*------------------------------------------------------------------------------*
* Summary statistics export
*------------------------------------------------------------------------------*

* By industry-year
preserve
    collapse (mean) mean_tfp_lp=omega_lp mean_tfp_wrdg=omega_wrdg mean_tfp_acf=omega_acf ///
             (sd) sd_tfp_lp=omega_lp sd_tfp_wrdg=omega_wrdg sd_tfp_acf=omega_acf ///
             (count) n_obs=omega_lp, ///
             by(ciiu_2d year)
    
    export delimited using "$output_dir/tfp_by_industry_year.csv", replace
restore

*------------------------------------------------------------------------------*
* Comparison table
*------------------------------------------------------------------------------*

preserve
    * Reshape to compare methods
    keep firm_id year ciiu_2d omega_lp omega_wrdg omega_acf
    
    * Only keep complete observations
    keep if !missing(omega_lp, omega_wrdg, omega_acf)
    
    * Calculate differences
    gen diff_lp_wrdg = omega_lp - omega_wrdg
    gen diff_lp_acf = omega_lp - omega_acf
    gen diff_wrdg_acf = omega_wrdg - omega_acf
    
    * Summary statistics
    collapse (mean) mean_diff_lp_wrdg=diff_lp_wrdg ///
                    mean_diff_lp_acf=diff_lp_acf ///
                    mean_diff_wrdg_acf=diff_wrdg_acf ///
             (sd) sd_diff_lp_wrdg=diff_lp_wrdg ///
                  sd_diff_lp_acf=diff_lp_acf ///
                  sd_diff_wrdg_acf=diff_wrdg_acf ///
             (count) n_obs=diff_lp_wrdg, ///
             by(ciiu_2d)
    
    export delimited using "$output_dir/tfp_method_comparison.csv", replace
restore

log close
