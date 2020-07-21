#!/usr/bin/env python3
import argparse
import os
import pyarrow as pa
import pandas as pd
import re
import sys
import traceback
from datetime import datetime

def convert_to_tile_arrow(in_file_list, out_dir, zoom, out_list_file, debug):
    warno = 189
    out_arrows = []
    res = 180 / 2**zoom
    cccc = ''
    form = ''
    cat_dir = ''
    date_hour = ''
    created = ''
    for in_file in in_file_list:
        loc_time_match = re.search(r'^.*/([A-Z][A-Z][A-Z][A-Z])/([^/]*)/(.*)/location_datetime/([0-9]*)/([0-9]*)\.arrow$', in_file)
        if loc_time_match:
            cccc = loc_time_match.group(1)
            form = loc_time_match.group(2)
            cat_dir = loc_time_match.group(3)
            date_hour = loc_time_match.group(4)
            created = loc_time_match.group(5)
            new_datetime_list_dict = {}
            new_id_list_dict = {}
            replace_id_list_dict = {}
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
                    new_datetime_list_dict[tile_x,  tile_y] = tile_df['datetime'].unique()
                    for datetime in new_datetime_list_dict[tile_x,  tile_y]:
                        new_df = tile_df[(datetime == tile_df['datetime'])]
                        if len(new_df['id'].tolist()) > 0:
                            out_directory = '/'.join([out_dir, form, cat_dir, 'indicator_location_datetime', str(zoom), str(tile_x), str(tile_y)])
                            out_file = ''.join([out_directory, '/', str(datetime.year).zfill(4), str(datetime.month).zfill(2), str(datetime.day).zfill(2), str(datetime.hour).zfill(2), '.arrow'])
                            new_id_list_dict[tile_x,  tile_y, datetime] = new_df['id'].tolist()
                            new_df.insert(0, 'indicator', cccc)
                            if os.path.exists(out_file):
                                old_df = pa.ipc.open_file(out_file).read_pandas()
                                concat_df = pd.concat([old_df, new_df])
                                unique_key_list = new_df.columns.values.tolist().remove('id')
                                duplicated = concat_df.duplicated(subset=unique_key_list)
                                keeped = concat_df.duplicated(subset=unique_key_list, keep='last')
                                new_df_duplicated_id_list = concat_df[duplicated]['id'].tolist()
                                old_df_keeped_id_list = concat_df[keeped]['id'].tolist()
                                replace_id_list_dict[tile_x,  tile_y, datetime] = [new_df_duplicated_id_list, old_df_keeped_id_list]
                                with open(out_file, 'bw') as out_f:
                                    writer = pa.ipc.new_file(out_f, pa.Schema.from_pandas(new_df))
                                    writer.write_table(pa.Table.from_pandas(concat_df[~duplicated]))
                                    writer.close()
                                if not out_file in out_arrows:
                                    out_arrows.append(out_file)
                            else:
                                os.makedirs(out_directory, exist_ok=True)
                                with open(out_file, 'bw') as out_f:
                                    writer = pa.ipc.new_file(out_f, pa.Schema.from_pandas(new_df))
                                    writer.write_table(pa.Table.from_pandas(new_df))
                                    writer.close()
                                if not out_file in out_arrows:
                                    out_arrows.append(out_file)
        else:
            prop_match = re.search(r'^.*/' + cccc + '/' + form + '/' + cat_dir + '/([^/]*)/' + date_hour + '/' + created + '\.arrow$', in_file)
            if prop_match:
                prop_short_name = prop_match.group(1)
                in_df = pa.ipc.open_file(in_file).read_pandas()
                for tile_x in range(0, 2**(zoom + 1)):
                    for tile_y in range(0, 2**(zoom)):
                        for datetime in new_datetime_list_dict[tile_x,  tile_y]:
                            if len(new_id_list_dict[tile_x,  tile_y, datetime]) > 0:
                                intersection_id_list = list(set(new_id_list_dict[tile_x,  tile_y, datetime]) & set(in_df['id'].tolist()))
                                new_df = in_df[in_df['id'].isin(intersection_id_list)]
                                if len(new_df['id'].tolist()) > 0:
                                    out_directory = '/'.join([out_dir, form, cat_dir, prop_short_name, str(zoom), str(tile_x), str(tile_y)])
                                    out_file = ''.join([out_directory, '/', str(datetime.year).zfill(4), str(datetime.month).zfill(2), str(datetime.day).zfill(2), str(datetime.hour).zfill(2), '.arrow'])
                                    new_df.insert(0, 'indicator', cccc)
                                    if os.path.exists(out_file):

                                        new_df['id'].replace(new_df_duplicated_id_list, old_df_keeped_id_list, inplace=True)

                                        old_df = pa.ipc.open_file(out_file).read_pandas()
                                        concat_df = pd.concat([old_df, new_df])
                                        duplicated = concat_df.duplicated(subset=['indicator', 'id'])
                                        with open(out_file, 'bw') as out_f:
                                            writer = pa.ipc.new_file(out_f, pa.Schema.from_pandas(new_df))
                                            writer.write_table(pa.Table.from_pandas(concat_df[~duplicated]))
                                            writer.close()
                                        if not out_file in out_arrows:
                                            out_arrows.append(out_file)
                                    else:
                                        os.makedirs(out_directory, exist_ok=True)
                                        with open(out_file, 'bw') as out_f:
                                            writer = pa.ipc.new_file(out_f, pa.Schema.from_pandas(new_df))
                                            writer.write_table(pa.Table.from_pandas(new_df))
                                            writer.close()
                                        if not out_file in out_arrows:
                                            out_arrows.append(out_file)

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
