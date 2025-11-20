/*==============================================================================
PROJECT: China Shock and Colombian Manufacturing
FILE: master.do
PURPOSE: Execute all data processing and analysis scripts in proper sequence

EXECUTION ORDER:
    00_process_banrep_deflators.do  -> Price indices
    01_merge_eam.do                 -> Panel construction
    02_prepare_for_analysis.do      -> Deflation and cleaning
    03_estimate_tfp_prodest.do      -> TFP estimation

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

do "$code_dir/00_process_banrep_deflators.do"

do "$code_dir/01_merge_eam.do"

do "$code_dir/02_prepare_for_analysis.do"

do "$code_dir/03_estimate_tfp_prodest.do"

*shell python "$code_dir/04_download_comtrade.py"

do "$code_dir/04_process_comtrade.do"
