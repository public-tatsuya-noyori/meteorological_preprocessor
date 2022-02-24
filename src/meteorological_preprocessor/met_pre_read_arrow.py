#!/usr/bin/env python3
import argparse
import os
import pandas as pd
import pyarrow as pa
import sys
import traceback

def null_int(t):
  if pa.types.is_integer(t):
    return pd.Int64Dtype()

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_arrow_file', type=str, metavar='input_arrow_file')
    parser.add_argument("--rows", action='store_true')
    parser.add_argument("--columns", action='store_true')
    parser.add_argument("--csv", action='store_true')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument('--hdf5', type=str, metavar='output.hdf5')
    args = parser.parse_args()
    if not os.access(args.input_arrow_file, os.F_OK):
        print('Error', errno, ':', args.input_arrow_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_arrow_file):
        print('Error', errno, ':', args.input_arrow_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_arrow_file, os.R_OK):
        print('Error', errno, ':', args.input_arrow_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        if args.rows:
            pd.options.display.max_rows = None
        if args.columns:
            pd.options.display.max_columns = None
        pd.options.display.float_format = '{:.6g}'.format
        ipc_reader_with_memory_map = pa.ipc.RecordBatchFileReader(pa.memory_map(args.input_arrow_file, 'r'))
        if args.csv:
            print(ipc_reader_with_memory_map.read_pandas(integer_object_nulls=True, types_mapper=null_int).to_csv(float_format='{:.6g}'.format))
        elif args.hdf5:
            ipc_reader_with_memory_map.read_pandas().to_hdf(args.hdf5, 'key', mode='w', complevel=9)
        else:
            print('num_record_batches:', ipc_reader_with_memory_map.num_record_batches)
            print('\nschema:')
            print(ipc_reader_with_memory_map.schema)
            print('\nread_pandas:')
            print(ipc_reader_with_memory_map.read_pandas(integer_object_nulls=True, types_mapper=null_int))
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
