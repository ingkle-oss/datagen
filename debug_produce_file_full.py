#!/usr/bin/env python3

import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.nazare import edge_load_datasources, edge_row_encode, edge_row_decode
from utils.utils import LoadRows

def debug_produce_file_full():
    """produce_file.py 전체 과정 디버깅"""
    print("=== Full Produce File Process Debug ===")
    
    # 1. CSV 파일 읽기 (LoadRows 사용 - 컨텍스트 매니저로)
    print("1. Loading CSV with LoadRows...")
    with LoadRows("testmct2.csv", "csv") as load_rows:
        # 첫 번째 행 읽기
        first_row = next(load_rows)
        print(f"First row type: {type(first_row)}")
        print(f"First row keys: {list(first_row.keys())}")
        
        # fanuc@ 필드만 선택
        fanuc_fields = {k: v for k, v in first_row.items() if k.startswith('fanuc@')}
        print(f"Fanuc fields count: {len(fanuc_fields)}")
        
        # 2. 스키마 로드
        print("\n2. Loading schema...")
        datasources = edge_load_datasources("testmct2.json", "json")
        
        # 3. Edge 인코딩
        print("\n3. Edge encoding...")
        encoded = edge_row_encode(fanuc_fields, datasources)
        print(f"Encoded length: {len(encoded)} bytes")
        
        # 4. Edge 디코딩
        print("\n4. Edge decoding...")
        decoded = edge_row_decode(encoded, datasources)
        
        # 5. 결과 확인
        print("\n5. Results:")
        string_fields = []
        for key, value in decoded.items():
            if isinstance(value, str):
                string_fields.append((key, value))
                print(f"  {key}: '{value}' (string)")
        
        print(f"\nString fields found: {len(string_fields)}")
        
        # 6. 원본과 비교
        print("\n6. Original vs Decoded comparison:")
        for key, value in decoded.items():
            if key in fanuc_fields:
                original = fanuc_fields[key]
                print(f"  {key}:")
                print(f"    Original: '{original}' (type: {type(original)})")
                print(f"    Decoded:  '{value}' (type: {type(value)})")
                print(f"    Match:    {original == value}")

if __name__ == "__main__":
    debug_produce_file_full()
