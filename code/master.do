/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: master.do
PURPOSE: Execute all data processing and analysis scripts in proper sequence

EXECUTION ORDER:
    PART 1: EAM Processing and TFP Estimation
    00_process_banrep_deflators.do  -> Price indices
    01_merge_eam.do                 -> Panel construction
    02_prepare_for_analysis.do      -> Deflation and cleaning
    03_standardize_ciiu.do          -> CIIU harmonization to ISIC Rev 4
    04_estimate_tfp_prodest.do      -> TFP estimation
    
    PART 2: Trade Data Processing
    05_download_comtrade.py         -> Download trade data (run separately)
    07_build_trade_concordances.do  -> Build HS6 to CIIU concordances
    06_process_comtrade.do          -> Process trade and map to CIIU

AUTHOR: David Becerra
DATE: November 2025
==============================================================================*/

clear all
set more off
set maxvar 10000
macro drop _all

global main_dir "C:\Users\dafrb\Desktop\EAM_data\CHINA-SCHOCK"
global raw_dir "$main_dir/raw_data"
global clean_dir "$main_dir/processed"
global output_dir "$main_dir/output"
global logs_dir "$main_dir/logs"
global code_dir "$main_dir/code"

foreach dir in main_dir raw_dir clean_dir output_dir logs_dir code_dir {
    capture mkdir "${`dir'}"
}

cd "$main_dir"

* =============================================================================
* PART 1: EAM PROCESSING AND TFP ESTIMATION
* =============================================================================

do "$code_dir/00_process_banrep_deflators.do"

do "$code_dir/01_merge_eam.do"

do "$code_dir/02_prepare_for_analysis.do"

do "$code_dir/03_standardize_ciiu.do"

do "$code_dir/04_estimate_tfp_prodest.do"

* =============================================================================
* PART 2: TRADE DATA PROCESSING
* =============================================================================

* Run Python script separately: python 05_download_comtrade.py

do "$code_dir/06_build_trade_concordances.do"

do "$code_dir/07_process_comtrade.do"
