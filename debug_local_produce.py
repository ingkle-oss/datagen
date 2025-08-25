#!/usr/bin/env python3

import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.nazare import edge_load_datasources, edge_row_encode, edge_row_decode

def debug_local_produce():
    """로컬에서 produce_file.py 과정 디버깅"""
    print("=== Local Produce File Debug ===")
    
    # CSV 파일 읽기
    df = pd.read_csv("testmct2.csv")
    print(f"CSV shape: {df.shape}")
    
    # 스키마 로드
    datasources = edge_load_datasources("testmct2.json", "json")
    
    # 첫 번째 행으로 테스트
    first_row = df.iloc[0].to_dict()
    fanuc_fields = {k: v for k, v in first_row.items() if k.startswith('fanuc@')}
    
    print(f"\nOriginal fanuc@ fields ({len(fanuc_fields)}):")
    for key, value in fanuc_fields.items():
        print(f"  {key}: '{value}' (type: {type(value)})")
    
    # Edge 인코딩 (produce_file.py에서 하는 것과 동일)
    print("\n=== Edge Encoding ===")
    encoded = edge_row_encode(fanuc_fields, datasources)
    print(f"Encoded length: {len(encoded)} bytes")
    
    # Edge 디코딩 (결과 확인용)
    print("\n=== Edge Decoding ===")
    decoded = edge_row_decode(encoded, datasources)
    
    print(f"\nDecoded fields ({len(decoded)}):")
    for key, value in decoded.items():
        print(f"  {key}: '{value}' (type: {type(value)})")
    
    # 원본과 비교
    print("\n=== Comparison ===")
    for key, value in decoded.items():
        if key in fanuc_fields:
            original = fanuc_fields[key]
            print(f"  {key}:")
            print(f"    Original: '{original}'")
            print(f"    Decoded:  '{value}'")
            print(f"    Match:    {original == value}")

if __name__ == "__main__":
    debug_local_produce()
