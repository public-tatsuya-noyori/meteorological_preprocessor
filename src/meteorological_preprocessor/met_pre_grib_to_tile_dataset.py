#!/usr/bin/env python3
import argparse
import copy
import math
import numpy as np
import os
import pandas as pd
import pkg_resources
import pyarrow as pa
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pyarrow import csv
from eccodes import *

def convert_to_dataset(cccc, cat, subcat, ft, in_df, out_dir, out_list_file, conf_df, debug):
    warno = 188
    created_second = int(math.floor(datetime.utcnow().timestamp()))
    for conf_tuple in conf_df[(conf_df['category'] == cat) & (conf_df['subcategory'] == subcat) & (conf_df['ft'] == ft)].itertuples():
        sort_unique_list = conf_tuple.sort_unique_list.split(';')
        tile_level = conf_tuple.tile_level
        new_datetime_list_dict = {}
        res = 180 / 2**tile_level
        for tile_x in range(0, 2**(tile_level + 1)):
            for tile_y in range(0, 2**(tile_level)):
                if tile_y == 2**(tile_level) - 1:
                    tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]'])]
                else:
                    tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]']) & (in_df['latitude [degree]'] > 90.0 - res * (tile_y + 1))]
                new_datetime_list_dict[tile_x,  tile_y] = tile_df['datetime'].dt.ceil(str(conf_tuple.minute_level) + 'T').unique()
                for new_datetime in new_datetime_list_dict[tile_x,  tile_y]:
                    new_df = tile_df[(new_datetime - timedelta(minutes=conf_tuple.minute_level) < tile_df['datetime']) & (tile_df['datetime'] <= new_datetime)]
                    out_file = ''.join([out_dir, '/', cccc, '/analysis_forecast/', cat, '/', subcat, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(new_datetime.hour).zfill(2), str(new_datetime.minute).zfill(2), '/', str(ft).zfill(4), '/l', str(tile_level), 'x', str(tile_x), 'y', str(tile_y), '.arrow'])
                    if len(new_df.index) > 0:
                        ctmdt_series = pd.to_datetime(new_df['datetime']) - pd.offsets.Second(created_second)
                        ctmdt_series = - ctmdt_series.map(pd.Timestamp.timestamp).astype(int)
                        new_head_df = pd.DataFrame({'created time minus data time [s]': ctmdt_series})
                        new_head_df = new_head_df.astype({'created time minus data time [s]': 'int32'})
                        new_head_df.insert(0, 'indicator', cccc)
                        new_head_df.astype({'indicator': 'string'})
                        new_df = pd.concat([new_head_df, new_df], axis=1)
                        tmp_sort_unique_list = list(set(new_df.columns) & set(sort_unique_list))
                        tmp_sort_unique_list.insert(0, 'indicator')
                        tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                        new_df.sort_values(tmp_sort_unique_list, inplace=True)
                        tmp_sort_unique_list.remove('created time minus data time [s]')
                        new_df.drop_duplicates(subset=tmp_sort_unique_list, keep='last', inplace=True)
                        tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                        os.makedirs(os.path.dirname(out_file), exist_ok=True)
                        table = pa.Table.from_pandas(new_df.reset_index(drop=True)).replace_schema_metadata(metadata=None)
                        with open(out_file, 'bw') as out_f:
                            #ipc_writer = pa.ipc.new_file(out_f, table.schema, options=pa.ipc.IpcWriteOptions(compression='zstd'))
                            ipc_writer = pa.ipc.new_file(out_f, table.schema, options=pa.ipc.IpcWriteOptions(compression=None))
                            for batch in table.to_batches():
                                ipc_writer.write_batch(batch)
                            ipc_writer.close()
                            print(out_file, file=out_list_file)

def convert_to_arrow(in_file_list, conf_df, out_dir, out_list_file, conf_grib_arrow_to_dataset_df, debug):
    warno = 189
    cccc_set = set([re.sub('^.*/', '', re.sub('/grib/.*$', '', in_file)) for in_file in in_file_list])
    cat_subcat_set = set([re.search(r'^[^/]*/[^/]*/', re.sub('^.*/grib/', '', in_file)).group().rstrip('/') for in_file in in_file_list])
    for cccc in cccc_set:
        for cat_subcat in cat_subcat_set:
            cat = re.sub('/.*$', '', cat_subcat)
            subcat = re.sub('^.*/', '', cat_subcat)
            convert = 'analysis_forecast'
            keys = ['stepRange', 'typeOfLevel', 'level', 'shortName']
            property_dict = {}
            dtype_dict = {}
            for in_file in in_file_list:
                match = re.search(r'^.*/' + cccc + '/grib/' + cat_subcat + '/.*$', in_file)
                if not match:
                    continue
                if not os.access(in_file, os.F_OK):
                    print('Warning', warno, ':', in_file, 'does not exist.', file=sys.stderr)
                    continue
                elif not os.path.isfile(in_file):
                    print('Warning', warno, ':', in_file, 'is not file.', file=sys.stderr)
                    continue
                elif not os.access(in_file, os.R_OK):
                    print('Warning', warno, ':', in_file, 'is not readable.', file=sys.stderr)
                    continue
                dt_str = re.sub('/.*$', '', re.sub('^.*/' + cccc + '/grib/' + cat_subcat + '/', '', in_file))
                with open(in_file, 'rb') as in_file_stream:
                    if debug:
                        print('Debug', ':', in_file, file=sys.stderr)
                    try:
                        codes_grib_multi_support_on()
                        while True:
                            gid = codes_grib_new_from_file(in_file_stream)
                            if gid is None:
                                break
                            bitmapPresent = codes_get(gid, "bitmapPresent")
                            if bitmapPresent:
                                codes_set(gid, "missingValue", 1e+20)
                            iterid = codes_keys_iterator_new(gid, 'ls')
                            step_range = None
                            type_of_level = None
                            level = None
                            short_name = None
                            target_conf_df = conf_df[(conf_df['category'] == cat) & (conf_df['subcategory'] == subcat)]
                            while codes_keys_iterator_next(iterid):
                                key = codes_keys_iterator_get_name(iterid)
                                if key in keys:
                                    value = codes_get_string(gid, key)
                                    if key == 'level':
                                        target_conf_df = target_conf_df[(target_conf_df[key] == int(value))]
                                    else:
                                        target_conf_df = target_conf_df[(target_conf_df[key] == str(value))]
                            #property_dict[(target_conf_df.iloc[0]['category'], target_conf_df.iloc[0]['subcategory'], target_conf_df.iloc[0]['stepRange'], target_conf_df.iloc[0]['typeOfLevel'], target_conf_df.iloc[0]['level'], target_conf_df.iloc[0]['shortName'], target_conf_df.iloc[0]['level_name'], target_conf_df.iloc[0]['ft'], target_conf_df.iloc[0]['name'], target_conf_df.iloc[0]['data_type'])] = np.array(codes_get_values(gid))
                            property_dict[(target_conf_df.iloc[0]['category'], target_conf_df.iloc[0]['level_name'], target_conf_df.iloc[0]['ft'], target_conf_df.iloc[0]['name'])] = np.array(codes_get_values(gid))
                            dtype_dict[(target_conf_df.iloc[0]['category'], target_conf_df.iloc[0]['level_name'], target_conf_df.iloc[0]['ft'], target_conf_df.iloc[0]['name'])] = target_conf_df.iloc[0]['data_type']
                            codes_keys_iterator_delete(iterid)
                            out_directory_list = [out_dir, cccc, convert, cat]
                            out_directory = '/'.join(out_directory_list)
                            out_file_list = [out_directory, '/', subcat, '.arrow']
                            out_file = ''.join(out_file_list)
                            if not os.path.exists(out_file):
                                iterid = codes_grib_iterator_new(gid, 0)
                                lat_list = []
                                lon_list = []
                                while True:
                                    latitude_longitude_value = codes_grib_iterator_next(iterid)
                                    if not latitude_longitude_value:
                                        break
                                    else:
                                        lat_list.append(latitude_longitude_value[0])
                                        if latitude_longitude_value[1] < 180.0:
                                            lon_list.append(latitude_longitude_value[1])
                                        else:
                                            lon_list.append(latitude_longitude_value[1] - 360.0)
                                codes_grib_iterator_delete(iterid)
                                os.makedirs(out_directory, exist_ok=True)
                                location_batch = pa.record_batch([pa.array(lat_list, 'float32'), pa.array(lon_list, 'float32')], names=['latitude [degree]', 'longitude [degree]'])
                                with open(out_file, 'bw') as out_f:
                                    ipc_writer = pa.ipc.new_file(out_f, location_batch.schema, options=pa.ipc.IpcWriteOptions(compression='zstd'))
                                    ipc_writer.write_batch(location_batch)
                                    ipc_writer.close()
                            codes_release(gid)
                    except:
                        traceback.print_exc(file=sys.stderr)
                        print('Warning', warno, ':', in_file, 'is invalid grib.', file=sys.stderr)
            if len(property_dict) > 0:
                out_directory_list = [out_dir, cccc, convert, cat]
                out_directory = '/'.join(out_directory_list)
                os.makedirs(out_directory, exist_ok=True)
                out_file_list = [out_directory, '/', subcat, '.arrow']
                out_file = ''.join(out_file_list)
                location_df = pa.ipc.open_file(out_file).read_pandas()
                dt = datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), 0, 0, 0, tzinfo=timezone.utc)
                dt_list = [dt for i in range(0, len(location_df.index))]
                schema = [pa.field('datetime', pa.timestamp('ms', tz='utc')), pa.field('latitude [degree]', 'float32'), pa.field('longitude [degree]', 'float32'), pa.field('id', 'string')]
                init_name_list = ['datetime', 'latitude [degree]', 'longitude [degree]','id']
                init_data_list = [pa.array(dt_list, pa.timestamp('ms', tz='utc')), pa.array(location_df['latitude [degree]'].values.tolist(), 'float32'), pa.array(location_df['longitude [degree]'].values.tolist(), 'float32')]
                level_list = []
                ft_list = []
                name_list = []
                property_dict2 = {}
                for property_key in property_dict.keys():
                    if property_key[1] not in level_list:
                        level_list = level_list + [property_key[1]]
                    if property_key[2] not in ft_list:
                        ft_list = ft_list + [property_key[2]]
                    if re.match(r'^U wind component$', property_key[3]):
                        u_value_np = property_dict[property_key]
                        v_value_np = property_dict[(property_key[0], property_key[1], property_key[2], property_key[3].replace('U', 'V'))]
                        wind_speed_np = np.sqrt(np.power(u_value_np, 2) + np.power(v_value_np, 2))
                        wind_direction_np = np.degrees(np.arctan2(v_value_np, u_value_np))
                        property_dict2[(property_key[0], property_key[1], property_key[2], 'wind speed [m s-1]')] = wind_speed_np.tolist()
                        dtype_dict[(property_key[0], property_key[1], property_key[2], 'wind speed [m s-1]')] = 'float32'
                        property_dict2[(property_key[0], property_key[1], property_key[2], 'wind direction [degree]')] = [value + 360.0 if value < 0 else value for value in wind_direction_np]
                        dtype_dict[(property_key[0], property_key[1], property_key[2], 'wind direction [degree]')] = 'float32'
                        if 'wind speed [m s-1]' not in name_list:
                            name_list.append('wind speed [m s-1]')
                        if 'wind direction [degree]' not in name_list:
                            name_list.append('wind direction [degree]')
                    elif not re.match(r'^V wind component$', property_key[3]):
                        if property_key[3] not in name_list:
                            name_list.append(property_key[3])
                none_data_list = [None for i in range(len(init_data_list[0]))]
                level_dict = {}
                for level in level_list:
                    level_dict[level] = pa.array([level for i in range(len(init_data_list[0]))], 'string')
                for ft in ft_list:
                    data_list = copy.copy(init_data_list)
                    tmp_name_list = []
                    for level in level_list:
                        data_list.append(level_dict[level])
                        for name in name_list:
                            if (cat, level, ft, name) in dtype_dict:
                                tmp_name_list.append(name)
                                if (cat, level, ft, name) in property_dict:
                                    data_list.append(pa.array(np.array(property_dict[(cat, level, ft, name)], dtype=dtype_dict[(cat, level, ft, name)])))
                                elif (cat, level, ft, name) in property_dict2:
                                    data_list.append(pa.array(np.array(property_dict2[(cat, level, ft, name)], dtype=dtype_dict[(cat, level, ft, name)])))
                                else:
                                    data_list.append(pa.array(np.array(none_data_list, dtype=dtype_dict[(cat, level, ft, name)])))
                    if 'surface' in level_list:
                        level_type = 'surface'
                    else:
                        level_type = 'upper_air'
                    names = init_name_list + tmp_name_list
                    batch = pa.record_batch(data_list, names=names)
                    convert_to_dataset(cccc, cat, level_type, ft, batch.to_pandas(), out_dir, out_list_file, conf_grib_arrow_to_dataset_df, debug)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument("--config_grib_to_arrow", type=str, metavar='conf_grib_to_arrow.csv', default=pkg_resources.resource_filename(__name__, 'conf_grib_to_arrow.csv'))
    parser.add_argument("--config_grib_arrow_to_dataset", type=str, metavar='conf_grib_arrow_to_dataset.csv', default=pkg_resources.resource_filename(__name__, 'conf_grib_arrow_to_dataset.csv'))
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(args.config_grib_to_arrow, os.F_OK):
        print('Error', errno, ':', args.config_grib_to_arrow, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_grib_arrow_to_dataset, os.F_OK):
        print('Error', errno, ':', args.config_grib_arrow_to_dataset, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config_grib_to_arrow):
        print('Error', errno, ':', args.config_grib_to_arrow, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config_grib_arrow_to_dataset):
        print('Error', errno, ':', args.config_grib_arrow_to_dataset, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_grib_to_arrow, os.R_OK):
        print('Error', errno, ':', args.config_grib_to_arrow, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_grib_arrow_to_dataset, os.R_OK):
        print('Error', errno, ':', args.config_grib_arrow_to_dataset, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        conf_grib_to_arrow_df = csv.read_csv(args.config_grib_to_arrow).to_pandas()
        conf_grib_arrow_to_dataset_df = csv.read_csv(args.config_grib_arrow_to_dataset).to_pandas()
        convert_to_arrow(input_file_list, conf_grib_to_arrow_df, args.output_directory, args.output_list_file, conf_grib_arrow_to_dataset_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
