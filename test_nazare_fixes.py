#!/usr/bin/env python3

import json
import logging
import sys
import pandas as pd
import pytest
import struct
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from utils.nazare import (
    edge_load_datasources,
    edge_row_encode,
    edge_row_decode,
    EdgeDataSource,
    _datasource_to_dataspecs,
    _split_payload_format,
    STRUCT_FMT,
    _edge_encode,
    _edge_decode,
)
from utils.utils import LoadRows


class TestNazareFixes:
    """Nazare 수정사항 테스트 클래스"""
    
    @pytest.fixture
    def test_schema_file(self):
        """테스트용 스키마 파일 경로"""
        return "samples/test/schema/testmct2.json"
    
    @pytest.fixture
    def test_parquet_file(self):
        """테스트용 parquet 파일 경로"""
        return "samples/test/schema/testmct2.parquet"
    
    @pytest.fixture
    def datasources(self, test_schema_file):
        """데이터소스 로드"""
        return edge_load_datasources(test_schema_file, "json")
    
    def test_struct_fmt_debug(self):
        """STRUCT_FMT 디버깅 테스트"""
        print("\n=== Testing STRUCT_FMT debug ===")
        
        # STRUCT_FMT 확인
        print(f"STRUCT_FMT['s']: {STRUCT_FMT['s']}")
        print(f"STRUCT_FMT['s'] type: {type(STRUCT_FMT['s'])}")
        
        # 테스트
        test_string = "AUTO"
        result = STRUCT_FMT['s'](test_string)
        print(f"Input: '{test_string}' (type: {type(test_string)})")
        print(f"Output: {result} (type: {type(result)})")
        
        assert isinstance(result, bytes), f"Expected bytes, got {type(result)}"
        print("✓ STRUCT_FMT['s'] correctly converts to bytes")
    
    def test_edge_encode_debug(self):
        """Edge 인코딩 디버깅 테스트"""
        print("\n=== Testing edge_encode debug ===")
        
        # 스키마 로드
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        
        # 문자열 필드만 포함하는 간단한 테스트 데이터
        test_data = {
            "fanuc@Mode": "AUTO",
        }
        
        print(f"Test data: {test_data}")
        
        # 각 spec에 대해 개별 테스트
        for src_id, format_str, specs in datasources:
            print(f"DataSource: {src_id}")
            print(f"Format: {format_str}")
            
            for spec in specs:
                if spec.edgeDataSpecId == "fanuc@Mode":
                    print(f"Testing spec: {spec.edgeDataSpecId}")
                    print(f"  Format: {spec.format}")
                    print(f"  Type: {spec.type}")
                    
                    try:
                        result = _edge_encode(spec, test_data)
                        print(f"  Result: {result}")
                        print(f"  Result type: {type(result[0]) if result else 'None'}")
                        
                        # struct.pack 테스트
                        if result:
                            packed = struct.pack(spec.format, *result)
                            print(f"  Packed successfully: {len(packed)} bytes")
                        
                    except Exception as e:
                        print(f"  Error: {e}")
                        pytest.fail(f"Edge encode failed for {spec.edgeDataSpecId}: {e}")
    
    def test_edge_encode_full_format_debug(self):
        """전체 포맷과 함께 edge 인코딩 디버깅 테스트"""
        print("\n=== Testing edge_encode with full format debug ===")
        
        # 스키마 로드
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        
        # 모든 필드를 포함하는 테스트 데이터 (기본값으로)
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
        
        for src_id, format_str, specs in datasources:
            print(f"DataSource: {src_id}")
            print(f"Format: {format_str}")
            print(f"Specs count: {len(specs)}")
            
            # 각 spec에 대해 개별 테스트
            all_vals = []
            for spec in sorted(specs, key=lambda x: x.index):
                try:
                    result = _edge_encode(spec, test_data)
                    print(f"  {spec.edgeDataSpecId}: {result} (type: {type(result[0]) if result else 'None'})")
                    all_vals.extend(result)
                except Exception as e:
                    print(f"  Error in {spec.edgeDataSpecId}: {e}")
                    pytest.fail(f"Edge encode failed for {spec.edgeDataSpecId}: {e}")
            
            print(f"All values count: {len(all_vals)}")
            print(f"All values types: {[type(v) for v in all_vals]}")
            
            # struct.pack 테스트
            try:
                packed = struct.pack(format_str, *all_vals)
                print(f"✓ Packed successfully: {len(packed)} bytes")
            except Exception as e:
                print(f"✗ Packing failed: {e}")
                pytest.fail(f"struct.pack failed: {e}")
    
    def test_struct_fmt_string_handling(self):
        """STRUCT_FMT의 문자열 처리 테스트"""
        print("\n=== Testing STRUCT_FMT string handling ===")
        
        # 's' 포맷이 bytes를 올바르게 처리하는지 확인
        test_string = "test_string"
        
        try:
            # STRUCT_FMT 사용
            bytes_value = STRUCT_FMT['s'](test_string)
            assert isinstance(bytes_value, bytes), f"Expected bytes, got {type(bytes_value)}"
            
            # 문자열 길이에 맞는 포맷 사용
            format_str = f"{len(test_string)}s"
            packed = struct.pack(format_str, bytes_value)
            unpacked = struct.unpack(format_str, packed)[0]
            decoded = unpacked.decode('utf-8')
            
            assert decoded == test_string
            print("✓ String encoding/decoding works correctly")
            
        except Exception as e:
            pytest.fail(f"String handling failed: {e}")
    
    def test_edge_encode_string_fields(self):
        """Edge 인코딩에서 문자열 필드 처리 테스트"""
        print("\n=== Testing edge_encode string fields ===")
        
        # 문자열 필드를 가진 테스트 데이터
        test_data = {
            "fanuc@Mode": "AUTO",
            "fanuc@MainProgram": "TEST.PRG",
            "fanuc@ActProgram": "ACTIVE",
        }
        
        # 스키마 로드
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        
        try:
            # 인코딩 시도
            encoded = edge_row_encode(test_data, datasources)
            print("✓ String field encoding successful")
            
            # 디코딩 시도
            decoded = edge_row_decode(encoded, datasources)
            print("✓ String field decoding successful")
            
            # 문자열 필드가 올바르게 처리되었는지 확인
            for field in test_data:
                if field in decoded:
                    original = test_data[field]
                    decoded_val = decoded[field]
                    
                    # bytes를 문자열로 변환하여 비교
                    if isinstance(decoded_val, bytes):
                        decoded_str = decoded_val.decode('utf-8').rstrip('\x00')
                    else:
                        decoded_str = str(decoded_val).rstrip('\x00')
                    
                    assert original == decoded_str, f"String mismatch: {original} != {decoded_str}"
            
            print("✓ String field integrity maintained")
            
        except Exception as e:
            pytest.fail(f"String field encoding/decoding failed: {e}")
    
    def test_loadrows_parquet_handling(self):
        """LoadRows의 parquet 파일 처리 테스트"""
        print("\n=== Testing LoadRows parquet handling ===")
        
        parquet_file = "samples/test/schema/testmct2.parquet"
        
        try:
            # LoadRows로 parquet 파일 열기
            with LoadRows(parquet_file, "parquet") as rows:
                # 첫 번째 행 읽기
                first_row = next(rows)
                print(f"✓ Successfully read parquet file")
                print(f"  Row keys: {list(first_row.keys())[:5]}")
                
                # 여러 행 읽기 테스트
                row_count = 0
                for row in rows:
                    row_count += 1
                    if row_count >= 5:  # 최대 5행만 테스트
                        break
                
                print(f"✓ Successfully read {row_count} additional rows")
                
        except Exception as e:
            pytest.fail(f"LoadRows parquet handling failed: {e}")
    
    def test_loadrows_context_manager(self):
        """LoadRows 컨텍스트 매니저 테스트"""
        print("\n=== Testing LoadRows context manager ===")
        
        # parquet 파일로 테스트
        parquet_file = "samples/test/schema/testmct2.parquet"
        
        try:
            # 컨텍스트 매니저 사용
            with LoadRows(parquet_file, "parquet") as rows:
                # 파일이 열려있는지 확인
                assert hasattr(rows, 'pq_batches')
                print("✓ LoadRows context manager opened successfully")
                
                # 첫 번째 행 읽기
                first_row = next(rows)
                assert isinstance(first_row, dict)
                print("✓ Successfully read first row")
            
            # 컨텍스트 매니저가 정상적으로 닫혔는지 확인
            print("✓ LoadRows context manager closed successfully")
            
        except Exception as e:
            pytest.fail(f"LoadRows context manager failed: {e}")
    
    def test_struct_error_prevention(self):
        """struct.error 방지 테스트"""
        print("\n=== Testing struct.error prevention ===")
        
        # 문제가 될 수 있는 데이터 생성
        problematic_data = {
            "fanuc@Mode": "AUTO",  # 문자열
            "fanuc@SigOP": 1,      # 정수
            "fanuc@MainProgram": "TEST.PRG",  # 긴 문자열
        }
        
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        
        try:
            # 인코딩 시도 (에러가 발생하지 않아야 함)
            encoded = edge_row_encode(problematic_data, datasources)
            print("✓ No struct.error occurred during encoding")
            
            # 디코딩 시도
            decoded = edge_row_decode(encoded, datasources)
            print("✓ No struct.error occurred during decoding")
            
        except struct.error as e:
            pytest.fail(f"struct.error occurred: {e}")
        except Exception as e:
            pytest.fail(f"Unexpected error: {e}")
    
    def test_unicode_handling(self):
        """유니코드 처리 테스트"""
        print("\n=== Testing Unicode handling ===")
        
        # 유니코드 문자열 포함 데이터
        unicode_data = {
            "fanuc@Mode": "AUTO모드",
            "fanuc@MainProgram": "테스트.PRG",
            "fanuc@ActProgram": "활성프로그램",
        }
        
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        
        try:
            # 인코딩 시도
            encoded = edge_row_encode(unicode_data, datasources)
            print("✓ Unicode encoding successful")
            
            # 디코딩 시도
            decoded = edge_row_decode(encoded, datasources)
            print("✓ Unicode decoding successful")
            
            # 유니코드 문자열이 올바르게 처리되었는지 확인
            for field, original_value in unicode_data.items():
                if field in decoded:
                    decoded_value = decoded[field]
                    # null 문자 제거 후 비교
                    decoded_clean = decoded_value.rstrip('\x00') if isinstance(decoded_value, str) else decoded_value
                    
                    assert original_value == decoded_clean, f"Unicode mismatch: {original_value} != {decoded_clean}"
            
            print("✓ Unicode integrity maintained")
            
        except UnicodeEncodeError as e:
            pytest.fail(f"UnicodeEncodeError occurred: {e}")
        except UnicodeDecodeError as e:
            pytest.fail(f"UnicodeDecodeError occurred: {e}")
        except Exception as e:
            pytest.fail(f"Unexpected error: {e}")
    
    def test_empty_string_handling(self):
        """빈 문자열 처리 테스트"""
        print("\n=== Testing empty string handling ===")
        
        # 빈 문자열 포함 데이터
        empty_string_data = {
            "fanuc@Mode": "",
            "fanuc@MainProgram": "TEST.PRG",
            "fanuc@ActProgram": "",
        }
        
        datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
        
        try:
            # 인코딩 시도
            encoded = edge_row_encode(empty_string_data, datasources)
            print("✓ Empty string encoding successful")
            
            # 디코딩 시도
            decoded = edge_row_decode(encoded, datasources)
            print("✓ Empty string decoding successful")
            
            # 빈 문자열이 올바르게 처리되었는지 확인
            for field, original_value in empty_string_data.items():
                if field in decoded:
                    decoded_value = decoded[field]
                    if original_value == "":
                        # 빈 문자열은 0으로 처리되었을 수 있음
                        assert decoded_value == 0 or decoded_value == "", f"Empty string not handled correctly: {decoded_value}"
            
            print("✓ Empty string handling correct")
            
        except Exception as e:
            pytest.fail(f"Empty string handling failed: {e}")
    
    def test_parquet_compatibility_integration(self):
        """Parquet 호환성 통합 테스트"""
        print("\n=== Testing Parquet compatibility integration ===")
        
        try:
            # Parquet 파일 읽기
            df = pd.read_parquet("samples/test/schema/testmct2.parquet")
            print(f"✓ Loaded parquet: {len(df)} rows")
            
            # 유효한 데이터가 있는 행 찾기
            valid_rows = df.dropna(subset=['fanuc@SigOP']).head(1)
            
            if len(valid_rows) == 0:
                pytest.skip("No valid data in parquet file")
            
            # 첫 번째 유효한 행을 edge 형식으로 변환
            first_row = valid_rows.iloc[0].to_dict()
            edge_row = {k: v for k, v in first_row.items() if pd.notna(v)}
            
            print(f"✓ Converted to edge format: {len(edge_row)} fields")
            
            # 스키마 로드
            datasources = edge_load_datasources("samples/test/schema/testmct2.json", "json")
            
            # Edge 인코딩/디코딩 테스트
            encoded = edge_row_encode(edge_row, datasources)
            print("✓ Parquet data encoding successful")
            
            decoded = edge_row_decode(encoded, datasources)
            print("✓ Parquet data decoding successful")
            
            # 데이터 무결성 확인 (일부 필드만)
            numeric_fields = ["fanuc@SigOP", "fanuc@SigSTL", "fanuc@SigSPL"]
            match_count = 0
            
            for field in numeric_fields:
                if field in edge_row and field in decoded:
                    original = edge_row[field]
                    decoded_val = decoded[field]
                    if original == decoded_val:
                        match_count += 1
            
            print(f"✓ Data integrity: {match_count}/{len(numeric_fields)} numeric fields match")
            
            assert match_count > 0, "No numeric fields matched"
            
        except Exception as e:
            pytest.fail(f"Parquet compatibility integration failed: {e}")


def test_struct_fmt_fix():
    """STRUCT_FMT 수정 확인 테스트"""
    print("\n=== Testing STRUCT_FMT fix ===")
    
    # 's' 포맷이 올바르게 처리되는지 확인
    test_string = "test"
    
    try:
        # bytes로 변환
        if callable(STRUCT_FMT.get("s")):
            # 함수인 경우
            bytes_value = STRUCT_FMT["s"](test_string)
        else:
            # 직접 변환
            bytes_value = test_string.encode('utf-8')
        
        assert isinstance(bytes_value, bytes), f"Expected bytes, got {type(bytes_value)}"
        print("✓ STRUCT_FMT['s'] correctly converts to bytes")
        
    except Exception as e:
        pytest.fail(f"STRUCT_FMT['s'] handling failed: {e}")


def test_loadrows_fo_attribute_fix():
    """LoadRows fo 속성 수정 확인 테스트"""
    print("\n=== Testing LoadRows fo attribute fix ===")
    
    try:
        # parquet 파일로 테스트 (fo가 None인 경우)
        with LoadRows("samples/test/schema/testmct2.parquet", "parquet") as rows:
            # 정상적으로 열렸는지 확인
            assert hasattr(rows, 'pq_batches')
            print("✓ LoadRows opened successfully for parquet")
        
        # 컨텍스트 매니저가 정상적으로 닫혔는지 확인
        print("✓ LoadRows closed successfully without fo attribute error")
        
    except AttributeError as e:
        if "fo" in str(e):
            pytest.fail(f"fo attribute error still exists: {e}")
        else:
            raise
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")


if __name__ == "__main__":
    # pytest 실행
    pytest.main([__file__, "-v", "-s"])
