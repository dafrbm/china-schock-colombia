"""
==============================================================================
UN COMTRADE DATA DOWNLOAD - Complete Dataset
Downloads all trade data required for China shock analysis

Downloads:
1. Colombia imports from China (HS6)
2. Colombia imports from World (HS6)
3. Colombia exports to World (HS6)
4. China exports to LAC countries (HS6)

Author: David Becerra
Date: November 2025
==============================================================================
"""

import pandas as pd
import requests
import time
import os
import sys
from datetime import datetime

# Configuration
OUTPUT_DIR = "C:/Users/dafrb/Desktop/EAM_data/raw_data/comtrade"
os.makedirs(OUTPUT_DIR, exist_ok=True)

API_BASE = "https://comtradeapi.un.org/data/v1/get"
API_KEY = "9889968aef8743358f4bca61dd62af53"

COUNTRIES = {
    'Colombia': '170',
    'China': '156',
    'World': '0',
    'Chile': '152',
    'Peru': '604',
    'Brazil': '076',
    'Argentina': '032',
    'Ecuador': '218'
}

call_counter = 0
max_calls = 500

def download_year(api_key, reporter, partner, year, flow, cmd_code='AG6'):
    """Download data for single year"""
    global call_counter
    
    url = f"{API_BASE}/C/A/HS"
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    params = {
        'reporterCode': reporter,
        'period': str(year),
        'partnerCode': partner,
        'flowCode': flow,
        'cmdCode': cmd_code
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        call_counter += 1
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                return pd.DataFrame(data['data'])
        elif response.status_code == 429:
            time.sleep(60)
            return download_year(api_key, reporter, partner, year, flow, cmd_code)
    except:
        pass
    
    return None

def download_dataset(api_key, reporter, partner, start_year, end_year, flow, cmd_code='AG6'):
    """Download complete dataset"""
    all_data = []
    
    for year in range(start_year, end_year + 1):
        df = download_year(api_key, reporter, partner, year, flow, cmd_code)
        if df is not None:
            all_data.append(df)
        time.sleep(1.5)
        
        if call_counter >= max_calls - 50:
            break
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return None

def main():
    global call_counter
    
    start_time = datetime.now()
    print(f"\nComtrade download started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Download 1: Colombia imports from China
    print("\n[1/4] Colombia imports from China (HS6)...")
    df = download_dataset(API_KEY, COUNTRIES['Colombia'], COUNTRIES['China'], 
                         1992, 2023, 'M', 'AG6')
    if df is not None:
        df.to_csv(os.path.join(OUTPUT_DIR, "colombia_imports_from_china_HS6.csv"), index=False)
        print(f"  Records: {len(df):,} | Calls: {call_counter}")
    
    # Download 2: Colombia imports from World
    print("\n[2/4] Colombia imports from World (HS6)...")
    df = download_dataset(API_KEY, COUNTRIES['Colombia'], COUNTRIES['World'], 
                         1992, 2023, 'M', 'AG6')
    if df is not None:
        df.to_csv(os.path.join(OUTPUT_DIR, "colombia_imports_from_world_HS6.csv"), index=False)
        print(f"  Records: {len(df):,} | Calls: {call_counter}")
    
    if call_counter >= max_calls - 50:
        print(f"\nApproaching daily limit. Exports pending for tomorrow.")
        sys.exit(0)
    
    # Download 3: Colombia exports to World
    print("\n[3/4] Colombia exports to World (HS6)...")
    df = download_dataset(API_KEY, COUNTRIES['Colombia'], COUNTRIES['World'], 
                         1992, 2023, 'X', 'AG6')
    if df is not None:
        df.to_csv(os.path.join(OUTPUT_DIR, "colombia_exports_to_world_HS6.csv"), index=False)
        print(f"  Records: {len(df):,} | Calls: {call_counter}")
    
    # Download 4: China exports to LAC
    print("\n[4/4] China exports to LAC countries (HS6)...")
    lac_data = []
    for country_name in ['Chile', 'Peru', 'Brazil', 'Argentina', 'Ecuador']:
        df = download_dataset(API_KEY, COUNTRIES['China'], COUNTRIES[country_name], 
                             1992, 2023, 'X', 'AG6')
        if df is not None:
            df['partner_country'] = country_name
            lac_data.append(df)
        time.sleep(2)
    
    if lac_data:
        df_lac = pd.concat(lac_data, ignore_index=True)
        df_lac.to_csv(os.path.join(OUTPUT_DIR, "china_exports_to_lac_HS6.csv"), index=False)
        print(f"  Records: {len(df_lac):,} | Calls: {call_counter}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).seconds / 60
    
    print(f"\nDownload complete: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total API calls: {call_counter}/{max_calls}")
    print(f"Duration: {duration:.1f} minutes")
    print(f"Output: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
