#!/usr/bin/env python3

import json
import logging
import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.nazare import (
    edge_load_datasources,
    edge_row_encode,
    edge_row_decode,
    _edge_encode,
    _edge_decode,
    EdgeDataSpec,
    EdgeDataSpecType,
)

def debug_edge_encode_decode():
    """Edge 인코딩/디코딩 과정 디버깅"""
    print("=== Edge Encode/Decode Debug ===")
    
    # 스키마 로드
    datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
    if not datasources:
        return False
    
    _, _, specs = datasources[0]
    
    # 간단한 테스트 데이터
    test_data = {
        "fanuc@Mode": "AUTO",
        "fanuc@MainProgram": "TEST.PRG",
        "fanuc@ActProgram": "ACTIVE",
    }
    
    print(f"Original test data: {test_data}")
    
    # 각 필드별로 개별 테스트
    for field_id, original_value in test_data.items():
        print(f"\n--- Testing field: {field_id} ---")
        print(f"Original value: '{original_value}' (type: {type(original_value)})")
        
        # 해당 필드의 spec 찾기
        spec = None
        for s in specs:
            if s.edgeDataSpecId == field_id:
                spec = s
                break
        
        if not spec:
            print(f"  ✗ Spec not found for {field_id}")
            continue
        
        print(f"Spec: {spec.edgeDataSpecId}")
        print(f"Type: {spec.type}")
        print(f"Format: {spec.format}")
        print(f"Size: {spec.size}")
        
        try:
            # 개별 인코딩 테스트
            encoded_values = _edge_encode(spec, {field_id: original_value})
            print(f"Encoded values: {encoded_values}")
            
            # 개별 디코딩 테스트
            decoded_dict = _edge_decode(spec, encoded_values[0] if encoded_values else None)
            print(f"Decoded dict: {decoded_dict}")
            
            if field_id in decoded_dict:
                decoded_value = decoded_dict[field_id]
                print(f"Decoded value: '{decoded_value}' (type: {type(decoded_value)})")
                
                # 비교
                if str(original_value).strip() == str(decoded_value).strip():
                    print(f"  ✓ Match!")
                else:
                    print(f"  ✗ Mismatch!")
                    print(f"    Original: '{original_value}'")
                    print(f"    Decoded:  '{decoded_value}'")
                    print(f"    Original bytes: {original_value.encode('utf-8')}")
                    print(f"    Decoded bytes: {decoded_value.encode('utf-8') if isinstance(decoded_value, str) else decoded_value}")
            else:
                print(f"  ✗ Field not found in decoded result")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    return True

def debug_struct_packing():
    """struct.pack/unpack 디버깅"""
    print("\n=== Struct Pack/Unpack Debug ===")
    
    import struct
    
    # 문자열 필드 테스트
    test_strings = ["AUTO", "TEST.PRG", "ACTIVE"]
    
    for test_str in test_strings:
        print(f"\n--- Testing string: '{test_str}' ---")
        
        # 8s format으로 테스트
        try:
            # 패딩된 문자열 생성 (8바이트)
            padded_str = test_str.ljust(8, '\x00')
            print(f"Padded string: {repr(padded_str)}")
            
            # struct.pack
            packed = struct.pack(">8s", padded_str.encode('utf-8'))
            print(f"Packed: {packed}")
            
            # struct.unpack
            unpacked = struct.unpack(">8s", packed)
            print(f"Unpacked: {unpacked}")
            
            # 디코딩
            decoded_str = unpacked[0].decode('utf-8')
            print(f"Decoded string: '{decoded_str}'")
            
            # null 문자 제거
            clean_str = decoded_str.split('\x00')[0]
            print(f"Clean string: '{clean_str}'")
            
            if test_str == clean_str:
                print("  ✓ Match!")
            else:
                print("  ✗ Mismatch!")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")

def debug_nazare_functions():
    """Nazare 함수들 디버깅"""
    print("\n=== Nazare Functions Debug ===")
    
    # 스키마 로드
    datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
    if not datasources:
        return False
    
    _, format_str, specs = datasources[0]
    
    print(f"Format string: {format_str}")
    print(f"Total specs: {len(specs)}")
    
    # TEXT 타입 필드들 찾기
    text_specs = [spec for spec in specs if spec.type == EdgeDataSpecType.TEXT]
    print(f"TEXT specs: {len(text_specs)}")
    
    for spec in text_specs[:3]:  # 처음 3개만
        print(f"\nTEXT spec: {spec.edgeDataSpecId}")
        print(f"  Format: {spec.format}")
        print(f"  Size: {spec.size}")
        
        # 테스트 데이터
        test_value = "TEST"
        print(f"  Test value: '{test_value}'")
        
        try:
            # 인코딩
            encoded = _edge_encode(spec, {spec.edgeDataSpecId: test_value})
            print(f"  Encoded: {encoded}")
            
            # 디코딩
            decoded = _edge_decode(spec, encoded[0] if encoded else None)
            print(f"  Decoded: {decoded}")
            
        except Exception as e:
            print(f"  Error: {e}")

def main():
    """메인 디버깅 함수"""
    logging.basicConfig(level=logging.DEBUG)
    
    debug_edge_encode_decode()
    debug_struct_packing()
    debug_nazare_functions()

if __name__ == "__main__":
    main()
