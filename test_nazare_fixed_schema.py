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
    EdgeDataSource,
    _datasource_to_dataspecs,
    _split_payload_format,
)

def test_schema_loading():
    """수정된 스키마 로딩 테스트"""
    print("=== Fixed Schema Loading Test ===")
    
    try:
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        print(f"✓ Successfully loaded {len(datasources)} datasources")
        
        for src_id, format_str, specs in datasources:
            print(f"  DataSource ID: {src_id}")
            print(f"  Format: {format_str}")
            print(f"  Specs count: {len(specs)}")
            
            # 스펙 정보 출력 (fanuc@ 접두사 확인)
            for spec in specs[:3]:
                print(f"    {spec.edgeDataSpecId}: {spec.type} ({spec.format})")
        
        return datasources
        
    except Exception as e:
        print(f"✗ Schema loading failed: {e}")
        return None

def test_format_parsing():
    """Format 파싱 테스트"""
    print("\n=== Format Parsing Test ===")
    
    with open("samples/test/schema/testmct2.json", "r") as f:
        schema_data = json.load(f)
    
    format_str = schema_data["payload"]["format"]
    fields = schema_data["payload"]["fields"]
    
    print(f"Format: {format_str}")
    print(f"Fields count: {len(fields)}")
    
    try:
        parsed_formats = _split_payload_format(format_str)
        print(f"✓ Parsed {len(parsed_formats)} format tokens")
        
        # Fields와 formats 매칭 확인
        if len(fields) == len(parsed_formats):
            print(f"✓ Fields and formats count match")
            for field, fmt in zip(fields, parsed_formats):
                print(f"  {field}: {fmt}")
        else:
            print(f"✗ Count mismatch: {len(fields)} fields vs {len(parsed_formats)} formats")
        
        return True
        
    except Exception as e:
        print(f"✗ Format parsing failed: {e}")
        return False

def test_parquet_compatibility():
    """Parquet 데이터와의 호환성 테스트"""
    print("\n=== Parquet Compatibility Test ===")
    
    # 스키마 로드
    datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
    if not datasources:
        return False
    
    # Parquet 파일 읽기
    parquet_file = "samples/test/schema/testmct2.parquet"
    
    try:
        df = pd.read_parquet(parquet_file)
        print(f"✓ Loaded parquet: {len(df)} rows, {len(df.columns)} columns")
        
        # 유효한 데이터가 있는 행 찾기
        valid_rows = df.dropna(subset=['fanuc@SigOP']).head(3)
        print(f"✓ Found {len(valid_rows)} valid rows")
        
        if len(valid_rows) == 0:
            print("✗ No valid data found")
            return False
        
        # 첫 번째 유효한 행을 edge 형식으로 변환
        first_row = valid_rows.iloc[0]
        print(f"  Sample row: {dict(list(first_row.items())[:5])}")
        
        # Parquet 데이터를 edge 형식으로 변환
        edge_row = {}
        for key, value in first_row.items():
            if pd.isna(value):
                continue
            
            # fanuc@ 접두사는 그대로 유지 (스키마가 이미 fanuc로 수정됨)
            edge_row[key] = value
        
        print(f"✓ Converted to edge format: {len(edge_row)} fields")
        
        # Edge 인코딩/디코딩 테스트
        try:
            encoded = edge_row_encode(edge_row, datasources)
            print(f"✓ Successfully encoded")
            
            decoded = edge_row_decode(encoded, datasources)
            print(f"✓ Successfully decoded")
            print(f"  Decoded fields: {len(decoded)}")
            
            return True
            
        except Exception as e:
            print(f"✗ Edge encoding/decoding failed: {e}")
            return False
        
    except Exception as e:
        print(f"✗ Parquet compatibility test failed: {e}")
        return False

def test_data_integrity():
    """데이터 무결성 테스트"""
    print("\n=== Data Integrity Test ===")
    
    # 스키마 로드
    datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
    if not datasources:
        return False
    
    # 스키마에 맞는 테스트 데이터 생성 (fanuc@ 접두사 사용)
    test_data = {
        "fanuc@SigOP": 1,
        "fanuc@SigSTL": 2,
        "fanuc@SigSPL": 3,
        "fanuc@SigAL": 4,
        "fanuc@EMG": 5,
        "fanuc@Mode": "AUTO",
        "fanuc@MainProgram": "TEST.PRG",
        "fanuc@ActProgram": "ACTIVE",
        "fanuc@Sequence": 100,
        "fanuc@ActS": 200,
        "fanuc@ActF": 300,
        "fanuc@ModalS": 400,
        "fanuc@ModalF": 500,
        "fanuc@ModalT": 600,
        "fanuc@ModalM": 700,
        "fanuc@ModalM2": 800,
        "fanuc@ModalM3": 900,
        "fanuc@PartsNum": 10,
        "fanuc@PartsNumAll": 100,
        "fanuc@PowOnTime": 3600,
        "fanuc@RunTime": 1800,
        "fanuc@CutTime": 900,
        "fanuc@SigCUT": 1,
        "fanuc@SigSBK": 0,
        "fanuc@SigDM00": 1,
        "fanuc@SigDM01": 0,
        "fanuc@SigMDRN": 1,
        "fanuc@Override": 100,
        "fanuc@AbsPos": "123.456",
        "fanuc@RelPos": "78.901",
        "fanuc@McnPos": "234.567",
        "fanuc@Alarm": "OK",
        "fanuc@SpindleLoad": 50,
        "fanuc@SpindleSpeed": 1000,
        "fanuc@SpindleTemp": 45,
        "fanuc@ModalG8": 1,
    }
    
    print(f"Test data: {len(test_data)} fields")
    
    try:
        # 인코딩
        encoded = edge_row_encode(test_data, datasources)
        print(f"✓ Successfully encoded")
        
        # 디코딩
        decoded = edge_row_decode(encoded, datasources)
        print(f"✓ Successfully decoded")
        
        # 데이터 무결성 확인 (숫자 필드만)
        numeric_fields = [
            "fanuc@SigOP", "fanuc@SigSTL", "fanuc@SigSPL", "fanuc@SigAL", "fanuc@EMG",
            "fanuc@Sequence", "fanuc@ActS", "fanuc@ActF", "fanuc@ModalS", "fanuc@ModalF",
            "fanuc@ModalT", "fanuc@ModalM", "fanuc@ModalM2", "fanuc@ModalM3",
            "fanuc@PartsNum", "fanuc@PartsNumAll", "fanuc@PowOnTime", "fanuc@RunTime",
            "fanuc@CutTime", "fanuc@SigCUT", "fanuc@SigSBK", "fanuc@SigDM00",
            "fanuc@SigDM01", "fanuc@SigMDRN", "fanuc@Override", "fanuc@SpindleLoad",
            "fanuc@SpindleSpeed", "fanuc@SpindleTemp", "fanuc@ModalG8"
        ]
        
        match_count = 0
        for field in numeric_fields:
            if field in test_data and field in decoded:
                original = test_data[field]
                decoded_val = decoded[field]
                if original == decoded_val:
                    match_count += 1
                else:
                    print(f"  Mismatch: {field} {original} != {decoded_val}")
        
        print(f"✓ Numeric fields match: {match_count}/{len(numeric_fields)}")
        
        # 문자열 필드는 null 문자 제거 후 비교
        string_fields = [
            "fanuc@Mode", "fanuc@MainProgram", "fanuc@ActProgram",
            "fanuc@AbsPos", "fanuc@RelPos", "fanuc@McnPos", "fanuc@Alarm"
        ]
        
        string_match_count = 0
        for field in string_fields:
            if field in test_data and field in decoded:
                original = test_data[field].strip()
                decoded_val = decoded[field].strip().split('\x00')[0]  # null 문자 제거
                if original == decoded_val:
                    string_match_count += 1
                else:
                    print(f"  String mismatch: {field} '{original}' != '{decoded_val}'")
        
        print(f"✓ String fields match: {string_match_count}/{len(string_fields)}")
        
        return match_count == len(numeric_fields) and string_match_count == len(string_fields)
        
    except Exception as e:
        print(f"✗ Data integrity test failed: {e}")
        return False

def test_pipeline_creation():
    """Pipeline 생성 테스트"""
    print("\n=== Pipeline Creation Test ===")
    
    try:
        with open("samples/test/schema/testmct2.json", "r") as f:
            schema_data = json.load(f)
        
        edge_datasource = EdgeDataSource.model_validate(schema_data)
        print(f"✓ Created EdgeDataSource")
        print(f"  Edge ID: {edge_datasource.edgeId}")
        print(f"  DataSource ID: {edge_datasource.edgeDataSourceId}")
        print(f"  Type: {edge_datasource.type}")
        
        dataspecs = _datasource_to_dataspecs(edge_datasource)
        print(f"✓ Generated {len(dataspecs)} DataSpecs")
        
        return True
        
    except Exception as e:
        print(f"✗ Pipeline creation failed: {e}")
        return False

def main():
    """메인 테스트 함수"""
    logging.basicConfig(level=logging.INFO)
    
    tests = [
        ("Schema Loading", test_schema_loading),
        ("Format Parsing", test_format_parsing),
        ("Parquet Compatibility", test_parquet_compatibility),
        ("Data Integrity", test_data_integrity),
        ("Pipeline Creation", test_pipeline_creation),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*50}")
            print(f"Running: {test_name}")
            print(f"{'='*50}")
            
            result = test_func()
            results.append((test_name, result, None))
            
            if result:
                print(f"✓ {test_name}: PASSED")
            else:
                print(f"✗ {test_name}: FAILED")
            
        except Exception as e:
            print(f"✗ {test_name}: FAILED - {e}")
            results.append((test_name, False, str(e)))
    
    # 결과 요약
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print(f"{'='*50}")
    
    passed = sum(1 for _, result, _ in results if result)
    total = len(results)
    
    for test_name, result, error in results:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name}: {status}")
        if error:
            print(f"  Error: {error}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Nazare is working correctly.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
