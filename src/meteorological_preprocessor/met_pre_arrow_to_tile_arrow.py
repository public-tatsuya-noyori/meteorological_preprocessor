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
import os
import pyarrow as pa
import pandas as pd
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone

def convert_to_tile_arrow(in_file_list, out_dir, zoom, out_list_file, debug):
    warno = 189
    out_arrows = []
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
    for in_file in in_file_list:
        if debug:
            print('Debug', ': in_file', in_file, file=sys.stderr)
        loc_time_match = re.search(r'^.*/([A-Z][A-Z][A-Z][A-Z])/([^/]*)/(.*)/([0-9]*)/C_([A-Z]{4})_([0-9]*)/location_datetime\.arrow$', in_file)
        if loc_time_match:
            cccc = loc_time_match.group(1)
            form = loc_time_match.group(2)
            cat_dir = loc_time_match.group(3)
            date_hourminute = loc_time_match.group(4)
            creator = loc_time_match.group(5)
            created = loc_time_match.group(6)
            created_second = int(math.floor(datetime(int(created[0:4]), int(created[4:6]), int(created[6:8]), int(created[8:10]), int(created[10:12]), int(created[12:14]), 0, tzinfo=timezone.utc).timestamp()))
            new_datetime_list_dict = {}
            new_id_etfo_dict = {}
            del_etfo_id_dict = {}
            in_df = pa.ipc.open_file(in_file).read_pandas()
            for tile_x in range(0, 2**(zoom + 1)):
                for tile_y in range(0, 2**(zoom)):
                    if tile_x == 0 and tile_y == 0:
                        tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] <= res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]']) & (in_df['latitude [degree]'] >= 90.0 - res * (tile_y + 1))]
                    elif tile_x == 0:
                        tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] <= res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y > in_df['latitude [degree]']) & (in_df['latitude [degree]'] >= 90.0 - res * (tile_y + 1))]
                    elif tile_y == 0:
                        tile_df = in_df[(res * tile_x - 180.0 < in_df['longitude [degree]']) & (in_df['longitude [degree]'] <= res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]']) & (in_df['latitude [degree]'] >= 90.0 - res * (tile_y + 1))]
                    else:
                        tile_df = in_df[(res * tile_x - 180.0 < in_df['longitude [degree]']) & (in_df['longitude [degree]'] <= res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y > in_df['latitude [degree]']) & (in_df['latitude [degree]'] >= 90.0 - res * (tile_y + 1))]
                    new_datetime_list_dict[tile_x,  tile_y] = tile_df['datetime'].dt.floor("10T").unique()
                    for new_datetime in new_datetime_list_dict[tile_x,  tile_y]:
                        new_df = tile_df[(new_datetime <= tile_df['datetime']) & (tile_df['datetime'] < new_datetime + timedelta(minutes=10))]
                        if len(new_df['id'].tolist()) > 0:
                            out_directory = ''.join([out_dir, '/', form, '/', cat_dir, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(new_datetime.hour).zfill(2), str(math.floor(new_datetime.minute / 10)), '0/', str(zoom), '/', str(tile_x), '/', str(tile_y)])
                            out_file = ''.join([out_directory, '/location_datetime.arrow'])
                            new_df = new_df.astype({'id': 'int32'})
                            new_df.insert(1, 'indicator', ord(cccc[0]) * 1000000 + ord(cccc[1]) * 10000 + ord(cccc[2]) * 100 + ord(cccc[3]))
                            new_df = new_df.astype({'indicator': 'int32'})
                            etfo_df = pd.to_datetime(new_df['datetime']) - pd.offsets.Second(created_second)
                            etfo_df = - etfo_df.map(pd.Timestamp.timestamp).astype(int)
                            new_df.insert(0, 'elapsed time [s]', etfo_df)
                            new_df = new_df.astype({'elapsed time [s]': 'int32'})
                            etfo_list = etfo_df.tolist()
                            tmp_id_etfo_dict = {}
                            old_df = new_df.iloc[0:0]
                            for index, id in enumerate(new_df['id'].tolist()):
                                tmp_id_etfo_dict[id] = etfo_list[index]
                            new_id_etfo_dict[(tile_x,  tile_y, new_datetime)] = tmp_id_etfo_dict
                            if out_file in out_file_dict:
                                old_df = out_file_dict[out_file]
                            elif os.path.exists(out_file):
                                old_df = pa.ipc.open_file(out_file).read_pandas()
                            if len(old_df['id'].tolist()) > 0:
                                concat_df = pd.concat([old_df, new_df], ignore_index=True)
                                concat_df = concat_df.astype({'id': 'int32'})
                                concat_df = concat_df.astype({'indicator': 'int32'})
                                concat_df = concat_df.astype({'elapsed time [s]': 'int32'})
                                unique_key_list = concat_df.columns.values.tolist()
                                unique_key_list.pop(2)#del id
                                unique_key_list.pop(0)#del etfo
                                duplicated = concat_df.duplicated(subset=unique_key_list, keep='last')
                                del_etfo_id_dict[(tile_x, tile_y, new_datetime)] = concat_df[duplicated][['elapsed time [s]', 'id']]
                                concat_df.drop_duplicates(subset=unique_key_list, keep='last', inplace=True)
                                if len(concat_df['id'].tolist()) > 0:
                                    out_file_dict[out_file] = concat_df
                                else:
                                    out_file_dict.pop(out_file)
                            else:
                                out_file_dict[out_file] = new_df
        else:
            prop_match = re.search(r'^.*/' + cccc + '/' + form + '/' + cat_dir + '/' + date_hourminute + '/C_' + creator + '_' + created + '/([^/]*)\.arrow$', in_file)
            if prop_match:
                prop_short_name = prop_match.group(1)
                in_df = pa.ipc.open_file(in_file).read_pandas()
                for tile_x in range(0, 2**(zoom + 1)):
                    for tile_y in range(0, 2**(zoom)):
                        for new_datetime in new_datetime_list_dict[tile_x,  tile_y]:
                            if len(new_id_etfo_dict[tile_x,  tile_y, new_datetime]) > 0:                                
                                intersection_id_list = list(set(new_id_etfo_dict[(tile_x,  tile_y, new_datetime)].keys()) & set(in_df['id'].tolist()))
                                new_df = in_df[in_df['id'].isin(intersection_id_list)]
                                if len(new_df['id'].tolist()) > 0:
                                    out_directory = ''.join([out_dir, '/', form, '/', cat_dir, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(new_datetime.hour).zfill(2), str(math.floor(new_datetime.minute / 10)), '0/', str(zoom), '/', str(tile_x), '/', str(tile_y)])
                                    out_file = ''.join([out_directory, '/', prop_short_name, '.arrow'])
                                    new_df = new_df.astype({'id': 'int32'})
                                    new_df.insert(1, 'indicator', ord(cccc[0]) * 1000000 + ord(cccc[1]) * 10000 + ord(cccc[2]) * 100 + ord(cccc[3]))
                                    new_df = new_df.astype({'indicator': 'int32'})
                                    tmp_etfo_list = []
                                    tmp_id_etfo_dict = new_id_etfo_dict[(tile_x,  tile_y, new_datetime)]
                                    for id in new_df['id'].tolist():
                                        tmp_etfo_list.append(tmp_id_etfo_dict[id])
                                    new_df.insert(0, 'elapsed time [s]', tmp_etfo_list)
                                    new_df = new_df.astype({'elapsed time [s]': 'int32'})
                                    old_df = new_df.iloc[0:0]
                                    if out_file in out_file_dict:
                                        old_df = out_file_dict[out_file]
                                    elif os.path.exists(out_file):
                                        old_df = pa.ipc.open_file(out_file).read_pandas()
                                    if len(old_df['id'].tolist()) > 0:
                                        concat_df = pd.concat([old_df, new_df], ignore_index=True)
                                        concat_df = concat_df.astype({'id': 'int32'})
                                        concat_df = concat_df.astype({'indicator': 'int32'})
                                        concat_df = concat_df.astype({'elapsed time [s]': 'int32'})
                                        del_index_list = []
                                        if (tile_x,  tile_y, new_datetime) in del_etfo_id_dict:
                                            for del_etfo_id in del_etfo_id_dict[(tile_x,  tile_y, new_datetime)].itertuples():
                                                for del_index in concat_df.index[(concat_df['elapsed time [s]'] == del_etfo_id[1]) & (concat_df['id'] == del_etfo_id[2])]:
                                                    del_index_list.append(del_index)
                                        if len(del_index_list) > 0:
                                            concat_df.drop(concat_df.index[del_index_list], inplace=True)
                                        unique_key_list = new_df.columns.values.tolist()
                                        concat_df.drop_duplicates(subset=unique_key_list, keep='last', inplace=True)
                                        if len(concat_df['id'].tolist()) > 0:
                                            out_file_dict[out_file] = concat_df
                                        else:
                                            out_file_dict.pop(out_file)
                                    else:
                                        out_file_dict[out_file] = new_df
    for out_file, out_df in out_file_dict.items():
        if len(out_df['id'].tolist()) > 0:
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            with open(out_file, 'bw') as out_f:
                writer = pa.ipc.new_file(out_f, pa.Schema.from_pandas(out_df))
                writer.write_table(pa.Table.from_pandas(out_df))
                writer.close()
            out_arrows.append(out_file)
    print('\n'.join(out_arrows), file=out_list_file)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('zoom', type=int, metavar='zoom')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    try:
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        convert_to_tile_arrow(input_file_list, args.output_directory, args.zoom, args.output_list_file, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
