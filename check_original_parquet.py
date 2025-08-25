#!/usr/bin/env python3

import pandas as pd
import sys
from pathlib import Path

def check_original_parquet():
    """원본 parquet 파일 확인"""
    print("=== Original Parquet File Check ===")
    
    parquet_file = "samples/test/schema/testmct2.parquet"
    
    try:
        df = pd.read_parquet(parquet_file)
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        print("\nFirst 3 rows:")
        print(df.head(3))
        
        print("\nString fields check:")
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            print(f"  {col}:")
            print(f"    Unique values: {df[col].unique()}")
            print(f"    Non-null count: {df[col].count()}")
            print(f"    Sample values: {df[col].head(3).tolist()}")
            
    except Exception as e:
        print(f"Error reading parquet file: {e}")

if __name__ == "__main__":
    check_original_parquet()
