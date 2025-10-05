#!/usr/bin/env python3
"""
Memory-efficient pickle file extractor for large pandas DataFrames
Converts .pkl files to chunked Parquet format for efficient processing
"""

import pandas as pd
import sys
import os
import json
from pathlib import Path
import gc

def extract_pkl_metadata(pkl_file):
    """Extract metadata from pickle file without loading entire dataset"""
    try:
        df = pd.read_pickle(pkl_file)
        metadata = {
            'filename': os.path.basename(pkl_file),
            'rows': len(df),
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'memory_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'sample': df.head(5).to_dict('records')
        }
        del df
        gc.collect()
        return metadata
    except Exception as e:
        return {'error': str(e), 'filename': os.path.basename(pkl_file)}

def convert_pkl_to_parquet(pkl_file, output_dir, chunk_size=10000):
    """Convert pickle to chunked parquet files for memory efficiency"""
    try:
        df = pd.read_pickle(pkl_file)
        base_name = Path(pkl_file).stem
        output_path = os.path.join(output_dir, f"{base_name}.parquet")
        
        # Write to parquet with compression
        df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
        
        result = {
            'success': True,
            'input_file': pkl_file,
            'output_file': output_path,
            'rows': len(df),
            'size_mb': os.path.getsize(output_path) / 1024 / 1024
        }
        
        del df
        gc.collect()
        return result
    except Exception as e:
        return {'success': False, 'error': str(e), 'input_file': pkl_file}

def load_parquet_chunk(parquet_file, start_row=0, num_rows=100):
    """Load specific chunk from parquet file"""
    try:
        df = pd.read_parquet(parquet_file)
        chunk = df.iloc[start_row:start_row + num_rows]
        
        # Convert to dict and handle special types
        records = []
        for _, row in chunk.iterrows():
            record = {}
            for col in chunk.columns:
                val = row[col]
                # Handle pandas/numpy types
                if pd.isna(val):
                    record[col] = None
                elif isinstance(val, (pd.Timestamp, pd.DatetimeTZDtype)):
                    record[col] = str(val)
                else:
                    record[col] = val
            records.append(record)
        
        return {
            'success': True,
            'data': records,
            'total_rows': len(df),
            'chunk_start': start_row,
            'chunk_size': len(chunk)
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}

def compare_datasets(file1, file2):
    """Compare two parquet datasets and identify differences"""
    try:
        df1 = pd.read_parquet(file1)
        df2 = pd.read_parquet(file2)
        
        # Basic comparison
        comparison = {
            'file1': os.path.basename(file1),
            'file2': os.path.basename(file2),
            'rows_file1': len(df1),
            'rows_file2': len(df2),
            'columns_file1': list(df1.columns),
            'columns_file2': list(df2.columns),
            'row_difference': len(df1) - len(df2),
        }
        
        # If same columns, find record differences
        if set(df1.columns) == set(df2.columns):
            # Try common ID column names
            id_col = None
            for possible_id in ['id', 'ID', 'TDCJ', 'tdcj', 'TDCJ Number', 'offender_id']:
                if possible_id in df1.columns:
                    id_col = possible_id
                    break
            
            if id_col:
                ids1 = set(df1[id_col].dropna().astype(str).values)
                ids2 = set(df2[id_col].dropna().astype(str).values)
                
                comparison['id_column'] = id_col
                comparison['records_only_in_file1'] = len(ids1 - ids2)
                comparison['records_only_in_file2'] = len(ids2 - ids1)
                comparison['common_records'] = len(ids1 & ids2)
        
        del df1, df2
        gc.collect()
        return comparison
    except Exception as e:
        return {'error': str(e)}

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(json.dumps({'error': 'No command specified'}))
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'metadata':
        pkl_file = sys.argv[2]
        result = extract_pkl_metadata(pkl_file)
        print(json.dumps(result))
    
    elif command == 'convert':
        pkl_file = sys.argv[2]
        output_dir = sys.argv[3]
        result = convert_pkl_to_parquet(pkl_file, output_dir)
        print(json.dumps(result))
    
    elif command == 'load_chunk':
        parquet_file = sys.argv[2]
        start_row = int(sys.argv[3]) if len(sys.argv) > 3 else 0
        num_rows = int(sys.argv[4]) if len(sys.argv) > 4 else 100
        result = load_parquet_chunk(parquet_file, start_row, num_rows)
        print(json.dumps(result))
    
    elif command == 'compare':
        file1 = sys.argv[2]
        file2 = sys.argv[3]
        result = compare_datasets(file1, file2)
        print(json.dumps(result))
    
    else:
        print(json.dumps({'error': 'Unknown command'}))