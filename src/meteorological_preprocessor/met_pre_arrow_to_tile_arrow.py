#!/usr/bin/env python3
#
# Copyright 2020-2021 Japan Meteorological Agency.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
#   Tatsuya Noyori - Japan Meteorological Agency - https://www.jma.go.jp
#

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
from pyarrow import csv, feather

def convert_to_tile_arrow(in_file_list, out_dir, zoom, out_list_file, conf_df, debug):
    warno = 189
    res = 180 / 2**zoom
    cccc = ''
    form = ''
    cat_dir = ''
    date_hourminute = ''
    creator = ''
    created = ''
    created_second = 0
    new_datetime_list_dict = {}
    new_id_etfo_dict = {}
    del_etfo_id_dict = {}
    out_file_dict = {}
    all_column_dict = {}
    convert_cat_subcat_set = set([re.sub(r'/[^/]*C_[A-Z]{4}_[0-9]*\.feather$', '', re.search(r'[^/]*/[^/]*/[^/]*/[^/]*\.feather$', in_file).group()) for in_file in in_file_list])
    for convert_cat_subcat in convert_cat_subcat_set:
        convert_cat_subcat_match = re.search(r'^([^/]*)/([^/]*)/([^/]*)$', convert_cat_subcat)
        convert = convert_cat_subcat_match.group(1)
        cat = convert_cat_subcat_match.group(2)
        subcat = convert_cat_subcat_match.group(3)
        convert_cat_subcat_df = conf_df[(conf_df['convert'] == convert) & (conf_df['category'] == cat) & (conf_df['subcategory'] == subcat)]
        for in_file in in_file_list:
            match = re.search(r'^.*/([A-Z][A-Z][A-Z][A-Z])/' + convert_cat_subcat+ '/[^/]*C_([A-Z]{4})_([0-9]*)\.feather$', in_file)
            if not match:
                continue
            if debug:
                print('Debug', ': in_file', in_file, file=sys.stderr)
            cccc = match.group(1)
            creator = match.group(2)
            created = match.group(3)
            created_second = int(math.floor(datetime(int(created[0:4]), int(created[4:6]), int(created[6:8]), int(created[8:10]), int(created[10:12]), int(created[12:14]), 0, tzinfo=timezone.utc).timestamp()))
            new_datetime_list_dict = {}
            new_id_etfo_dict = {}
            del_dict = {}
            in_df = feather.read_feather(in_file)
            for tile_x in range(0, 2**(zoom + 1)):
                for tile_y in range(0, 2**(zoom)):
                    if tile_y == 2**(zoom) - 1:
                        tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]'])]
                    else:
                        tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]']) & (in_df['latitude [degree]'] > 90.0 - res * (tile_y + 1))]
                    new_datetime_list_dict[tile_x,  tile_y] = tile_df['datetime'].dt.floor("10T").unique()
                    for new_datetime in new_datetime_list_dict[tile_x,  tile_y]:
                        sort_key_list = convert_cat_subcat_df[(convert_cat_subcat_df['location_datetime'] == 1)]['name'].values.tolist()
                        new_df = tile_df[(new_datetime <= tile_df['datetime']) & (tile_df['datetime'] < new_datetime + timedelta(minutes=10))].dropna(subset=sort_key_list).dropna(axis=1, how='all')
                        if len(new_df.index) > 0:
                            out_directory = ''.join([out_dir, '/', convert_cat_subcat, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(new_datetime.hour).zfill(2), str(math.floor(new_datetime.minute / 10)), '0/', str(zoom), '/', str(tile_x), '/', str(tile_y)])
                            ssc_df = pd.to_datetime(new_df['datetime']) - pd.offsets.Second(created_second)
                            ssc_df = - ssc_df.map(pd.Timestamp.timestamp).astype(int)
                            new_df.insert(0, 'time since created [s]', ssc_df)
                            new_df = new_df.astype({'time since created [s]': 'int32'})
                            new_df.insert(0, 'indicator', ord(cccc[0]) * 1000000 + ord(cccc[1]) * 10000 + ord(cccc[2]) * 100 + ord(cccc[3]))
                            new_df = new_df.astype({'indicator': 'int32'})
                            sort_key_list.insert(0, 'indicator')
                            sort_key_list.insert(1, 'time since created [s]')
                            new_df.sort_values(sort_key_list, inplace=True)
                            sort_key_list.remove('time since created [s]')
                            new_df.drop_duplicates(subset=sort_key_list, keep='last', inplace=True)
                            sort_key_list.insert(1, 'time since created [s]')
                            new_df.reset_index(drop=True, inplace=True)
                            out_file = ''.join([out_directory, '/location_datetime.feather'])
                            properties_df = convert_cat_subcat_df[(convert_cat_subcat_df['location_datetime'] == 0)]
                            if out_directory in all_column_dict:
                                old_df = all_column_dict[out_directory]
                            elif os.path.exists(out_file):
                                old_df = feather.read_feather(out_file)
                                for column in properties_df['name'].values.tolist():
                                    old_df.join(feather.read_feather(''.join([out_directory, '/', column.split('[')[0].strip(' ').replace(' ', '_').replace('/', '_'), '.feather'])))
                            else:
                                old_df = new_df.iloc[0:0]
                            if len(old_df.index) > 0:
                                concat_df = pd.concat([old_df, new_df])
                                concat_df.sort_values(sort_key_list, inplace=True)
                                sort_key_list.remove('time since created [s]')
                                concat_df.drop_duplicates(subset=sort_key_list, keep='last', inplace=True)
                                sort_key_list.insert(1, 'time since created [s]')
                                concat_df.reset_index(drop=True, inplace=True)
                                all_column_dict[out_directory] = concat_df
                                out_file_dict[out_file] = concat_df[sort_key_list]
                                for column in properties_df['name'].values.tolist():
                                    out_file = ''.join([out_directory, '/', column.split('[')[0].strip(' ').replace(' ', '_').replace('/', '_'), '.feather'])
                                    out_file_dict[out_file] = concat_df[[column]]
                            else:
                                all_column_dict[out_directory] = new_df
                                out_file_dict[out_file] = new_df[sort_key_list]
                                for column in properties_df['name'].values.tolist():
                                    out_file = ''.join([out_directory, '/', column.split('[')[0].strip(' ').replace(' ', '_').replace('/', '_'), '.feather'])
                                    out_file_dict[out_file] = new_df[[column]]
    for out_file, out_df in out_file_dict.items():
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        table = pa.Table.from_pandas(out_df)
        with open(out_file, 'bw') as out_f:
            feather.write_feather(table, out_f, compression='zstd', compression_level=15)
        print(out_file, file=out_list_file)
        out_file = re.sub(r'\.feather', '.arrow', out_file)
        with open(out_file, 'bw') as out_f:
            feather.write_feather(table, out_f, compression='uncompressed')
        os.system("gzip {}".format(out_file))

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('zoom', type=int, metavar='zoom')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    config = pkg_resources.resource_filename(__name__, 'conf_arrow_to_tile_arrow.csv')
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
        convert_to_tile_arrow(input_file_list, args.output_directory, args.zoom, args.output_list_file, conf_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
