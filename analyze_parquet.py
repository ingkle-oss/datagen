#!/usr/bin/env python3

import pandas as pd
import sys

def analyze_parquet():
    """Parquet 파일 구조 분석"""
    parquet_file = "samples/test/schema/testmct2.parquet"
    
    try:
        # Parquet 파일 읽기
        df = pd.read_parquet(parquet_file)
        
        print(f"=== Parquet File Analysis ===")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print(f"Columns: {list(df.columns)}")
        
        # 첫 번째 행 확인
        first_row = df.iloc[0]
        print(f"\nFirst row values:")
        for col in df.columns:
            value = first_row[col]
            print(f"  {col}: {value} (type: {type(value)})")
        
        # 데이터 타입 확인
        print(f"\nData types:")
        for col in df.columns:
            dtype = df[col].dtype
            non_null_count = df[col].count()
            null_count = df[col].isna().sum()
            print(f"  {col}: {dtype} (non-null: {non_null_count}, null: {null_count})")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    analyze_parquet()
