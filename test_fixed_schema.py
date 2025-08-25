#!/usr/bin/env python3

import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.nazare import edge_load_datasources, edge_row_encode, edge_row_decode

def test_fixed_schema():
    """수정된 스키마로 Edge 인코딩 테스트"""
    print("=== Testing Fixed Schema ===")
    
    # CSV 파일 읽기
    df = pd.read_csv("testmct2.csv")
    
    # 스키마 로드
    datasources = edge_load_datasources("testmct2.json", "json")
    
    # fanuc@ 필드만 선택 (첫 번째 행)
    fanuc_fields = {k: v for k, v in df.iloc[0].items() if k.startswith('fanuc@')}
    
    print("Original fanuc@ data:")
    for key, value in fanuc_fields.items():
        print(f"  {key}: '{value}' (type: {type(value)})")
    
    # Edge 인코딩
    print("\nEncoding...")
    encoded = edge_row_encode(fanuc_fields, datasources)
    print(f"Encoded length: {len(encoded)} bytes")
    
    # Edge 디코딩
    print("\nDecoding...")
    decoded = edge_row_decode(encoded, datasources)
    
    print("\nDecoded data:")
    for key, value in decoded.items():
        if key in fanuc_fields:
            original = fanuc_fields[key]
            print(f"  {key}:")
            print(f"    Original: '{original}' (type: {type(original)})")
            print(f"    Decoded:  '{value}' (type: {type(value)})")
            print(f"    Match:    {original == value}")

if __name__ == "__main__":
    test_fixed_schema()
