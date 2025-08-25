#!/usr/bin/env python3

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.nzfake import _gen_struct_to_value

def test_string_generation():
    """문자열 생성 테스트"""
    print("=== String Generation Test ===")
    
    # 문자열 포맷 테스트
    test_formats = [
        ("8s", 8),   # 8바이트 문자열
        ("64s", 64), # 64바이트 문자열
        ("10s", 10), # 10바이트 문자열
        ("48s", 48), # 48바이트 문자열
    ]
    
    for format_str, size in test_formats:
        print(f"\nTesting format: {format_str} (size: {size})")
        for i in range(3):
            value = _gen_struct_to_value(format_str, size)
            if isinstance(value, bytes):
                str_value = value.decode('utf-8').rstrip('\x00')
                print(f"  Sample {i+1}: '{str_value}' (length: {len(str_value)})")
            else:
                print(f"  Sample {i+1}: {value}")

if __name__ == "__main__":
    test_string_generation()
