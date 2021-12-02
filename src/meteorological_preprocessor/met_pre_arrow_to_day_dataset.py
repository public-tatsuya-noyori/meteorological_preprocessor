#!/usr/bin/env python3
import argparse
import math
import numpy as np
import os
import pkg_resources
import pyarrow as pa
import pandas as pd
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pyarrow import csv

def convert_to_dataset(in_file_list, out_dir, out_list_file, conf_df, debug):
    warno = 189
    for conf_tuple in conf_df.itertuples():
        output_tile_level_list = [conf_tuple.day_tile_level]
        output_time_level_list = ['1D']
        output_child_directory_list = ['1DayDataset']
        output_list = conf_tuple.output_list.split(';')
        sort_unique_list = conf_tuple.sort_unique_list.split(';')
        out_file_dict = {}
        for in_file in in_file_list:
            match = re.search(r'^.*/([A-Z][A-Z0-9]{3})/' + conf_tuple.convert + '/' + conf_tuple.category + '/' + conf_tuple.subcategory + '/[^/]*C_[A-Z][A-Z0-9]{3}_([0-9]*)\.arrow$', in_file)
            if not match:
                continue
            if debug:
                print('Debug', ': in_file', in_file, file=sys.stderr)
            cccc = match.group(1)
            created = match.group(2)
            created_second = int(math.floor(datetime(int(created[0:4]), int(created[4:6]), int(created[6:8]), int(created[8:10]), int(created[10:12]), int(created[12:14]), 0, tzinfo=timezone.utc).timestamp()))
            in_ipc_reader = pa.ipc.open_file(in_file)
            in_df = in_ipc_reader.read_pandas()
            for output_index, tile_level in enumerate(output_tile_level_list):
                new_datetime_list_dict = {}
                res = 180 / 2**tile_level
                for tile_x in range(0, 2**(tile_level + 1)):
                    for tile_y in range(0, 2**(tile_level)):
                        if tile_y == 2**(tile_level) - 1:
                            tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]'])]
                        else:
                            tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]']) & (in_df['latitude [degree]'] > 90.0 - res * (tile_y + 1))]
                        new_datetime_list_dict[tile_x,  tile_y] = tile_df['datetime'].dt.floor(output_time_level_list[output_index]).unique()
                        for new_datetime in new_datetime_list_dict[tile_x,  tile_y]:
                            if output_child_directory_list[output_index] =='1MinuteDataset':
                                new_df = tile_df[(new_datetime <= tile_df['datetime']) & (tile_df['datetime'] < new_datetime + timedelta(minutes=1))]
                                out_directory = ''.join([out_dir, '/', output_child_directory_list[output_index], '/', conf_tuple.convert, '/', conf_tuple.category, '/', conf_tuple.subcategory, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(new_datetime.hour).zfill(2), str(new_datetime.minute).zfill(2), '/', str(tile_level), '/', str(tile_x), '/', str(tile_y)])
                            elif output_child_directory_list[output_index] =='1DayDataset':
                                new_df = tile_df[(new_datetime <= tile_df['datetime']) & (tile_df['datetime'] < new_datetime + timedelta(days=1))]
                                out_directory = ''.join([out_dir, '/', output_child_directory_list[output_index], '/', conf_tuple.convert, '/', conf_tuple.category, '/', conf_tuple.subcategory, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(tile_level), '/', str(tile_x), '/', str(tile_y)])
                            if len(new_df.index) > 0:
                                ssc_df = pd.to_datetime(new_df['datetime']) - pd.offsets.Second(created_second)
                                ssc_df = - ssc_df.map(pd.Timestamp.timestamp).astype(int)
                                new_df.insert(0, 'created time minus data time [s]', ssc_df)
                                new_df = new_df.astype({'created time minus data time [s]': 'int32'})
                                new_df.insert(0, 'indicator', cccc)
                                new_df = new_df.astype({'indicator': 'string'})
                                tmp_sort_unique_list = list(set(new_df.columns) & set(sort_unique_list))
                                tmp_sort_unique_list.insert(0, 'indicator')
                                tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                                new_df.sort_values(tmp_sort_unique_list, inplace=True)
                                tmp_sort_unique_list.remove('created time minus data time [s]')
                                new_df.drop_duplicates(subset=tmp_sort_unique_list, keep='last', inplace=True)
                                tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                                new_df.reset_index(drop=True, inplace=True)
                                out_file = ''.join([out_directory, '/', conf_tuple.output_name])
                                if out_file in out_file_dict:
                                    former_df = out_file_dict[out_file]
                                elif os.path.exists(out_file):
                                    former_ipc_reader = pa.ipc.open_file(out_file)
                                    former_df = former_ipc_reader.read_pandas()
                                else:
                                    former_df = pd.DataFrame()
                                if len(former_df.index) > 0:
                                    new_df = pd.concat([former_df, new_df])
                                    tmp_sort_unique_list = list(set(new_df.columns) & set(sort_unique_list))
                                    tmp_sort_unique_list.insert(0, 'indicator')
                                    tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                                    new_df.sort_values(tmp_sort_unique_list, inplace=True)
                                    tmp_sort_unique_list.remove('created time minus data time [s]')
                                    new_df.drop_duplicates(subset=tmp_sort_unique_list, keep='last', inplace=True)
                                    tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                                    new_df.reset_index(drop=True, inplace=True)
                                out_file_dict[out_file] = new_df
        for out_file, out_df in out_file_dict.items():
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            table = pa.Table.from_pandas(out_df).replace_schema_metadata(metadata=None)
            with open(out_file, 'bw') as out_f:
                #ipc_writer = pa.ipc.new_file(out_f, table.schema, options=pa.ipc.IpcWriteOptions(compression='zstd'))
                ipc_writer = pa.ipc.new_file(out_f, table.schema, options=pa.ipc.IpcWriteOptions(compression=None))
                for batch in table.to_batches():
                    ipc_writer.write_batch(batch)
                ipc_writer.close()
                print(out_file, file=out_list_file)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    config = pkg_resources.resource_filename(__name__, 'conf_arrow_to_dataset.csv')
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(config, os.F_OK):
        print('Error', errno, ':', config, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(config):
        print('Error', errno, ':', config, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(config, os.R_OK):
        print('Error', errno, ':', config, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        conf_df = csv.read_csv(config).to_pandas()
        convert_to_dataset(input_file_list, args.output_directory, args.output_list_file, conf_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
