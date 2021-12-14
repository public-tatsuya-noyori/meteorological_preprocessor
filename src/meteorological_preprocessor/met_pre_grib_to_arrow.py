#!/usr/bin/env python3
import argparse
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

def convert_to_arrow(my_cccc, in_file_list, out_dir, out_list_file, conf_df, write_location, debug):
    warno = 189
    out_arrows = []
    now = datetime.utcnow()
    create_datetime_list = ['C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)]
    create_datetime = ''.join(create_datetime_list)
    cccc_set = set([re.sub('^.*/', '', re.sub('/grib/.*$', '', in_file)) for in_file in in_file_list])
    cat_subcat_set = set([re.search(r'^[^/]*/[^/]*/', re.sub('^.*/grib/', '', in_file)).group().rstrip('/') for in_file in in_file_list])
    for cccc in cccc_set:
        for cat_subcat in cat_subcat_set:
            keys = ['stepRange', 'typeOfLevel', 'level', 'shortName']
            for in_file in in_file_list:
                property_dict = {}
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
                        iid = codes_index_new_from_file(in_file, keys)
                        key_values_list = []
                        for key in keys:
                            key_values = codes_index_get(iid, key)
                            key_values_list.append(key_values)
                        products = [[]]
                        for key_values in key_values_list:
                            products = [x + [y] for x in products for y in key_values]
                        for product in products:
                            for key_count in range(len(keys)):
                                codes_index_select(iid, keys[key_count], product[key_count])
                            while True:
                                #gid = codes_new_from_index(iid)
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
                                cat = re.sub('/.*$', '', cat_subcat)
                                subcat = re.sub('^.*/', '', cat_subcat)
                                target_conf_df = conf_df[(conf_df['category'] == cat) & (conf_df['subcategory'] == subcat)]
                                while codes_keys_iterator_next(iterid):
                                    key = codes_keys_iterator_get_name(iterid)
                                    if key in keys:
                                        value = codes_get_string(gid, key)
                                        if key == 'level':
                                            target_conf_df = target_conf_df[(target_conf_df[key] == int(value))]
                                        else:
                                            target_conf_df = target_conf_df[(target_conf_df[key] == value)]
                                property_dict[(target_conf_df.iloc[0]['category'], target_conf_df.iloc[0]['subcategory'], target_conf_df.iloc[0]['stepRange'], target_conf_df.iloc[0]['typeOfLevel'], target_conf_df.iloc[0]['level'], target_conf_df.iloc[0]['shortName'], target_conf_df.iloc[0]['level_name'], target_conf_df.iloc[0]['ft'], target_conf_df.iloc[0]['name'], target_conf_df.iloc[0]['data_type'])] = np.array(codes_get_values(gid))
                                codes_keys_iterator_delete(iterid)
                                if write_location:
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
                                    out_directory_list = [out_dir, cccc, 'grib_to_arrow', cat_subcat]
                                    out_directory = '/'.join(out_directory_list)
                                    os.makedirs(out_directory, exist_ok=True)
                                    out_file_list = [out_directory, '/location.arrow']
                                    out_file = ''.join(out_file_list)
                                    location_batch = pa.record_batch([pa.array(lat_list, 'float32'), pa.array(lon_list, 'float32')], names=['latitude [degree]', 'longitude [degree]'])
                                    with open(out_file, 'bw') as out_f:
                                        ipc_writer = pa.ipc.new_file(out_f, location_batch.schema, options=pa.ipc.IpcWriteOptions(compression='zstd'))
                                        ipc_writer.write_batch(location_batch)
                                        ipc_writer.close()
                                codes_release(gid)
                        codes_index_release(iid)
                    except:
                        print('Warning', warno, ':', in_file, 'is invalid grib.', file=sys.stderr)
                if len(property_dict) > 0:
                    out_directory_list = [out_dir, cccc, 'grib_to_arrow', cat_subcat]
                    out_directory = '/'.join(out_directory_list)
                    os.makedirs(out_directory, exist_ok=True)
                    out_file_list = [out_directory, '/location.arrow']
                    out_file = ''.join(out_file_list)
                    location_df = pa.ipc.open_file(out_file).read_pandas()
                    dt = datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), 0, 0, 0, tzinfo=timezone.utc)
                    dt_list = [dt for i in range(0, len(location_df.index))]
                    name_list = ['latitude [degree]', 'longitude [degree]', 'datetime']
                    data_list = [pa.array(location_df['latitude [degree]'].values.tolist(), 'float32'), pa.array(location_df['longitude [degree]'].values.tolist(), 'float32'), pa.array(dt_list, pa.timestamp('ms', tz='utc'))]
                    out_name_dict = {}
                    out_data_dict = {}
                    for property_key in property_dict.keys():
                        level_ft_out_directory = '/'.join([out_directory, property_key[6], str(property_key[7])])
                        os.makedirs(level_ft_out_directory, exist_ok=True)
                        out_file_list = [level_ft_out_directory, '/', dt_str, '_', create_datetime, '.arrow']
                        out_file = ''.join(out_file_list)
                        if re.match(r'^U wind component$', property_key[8]):
                            u_value_np = property_dict[property_key]
                            v_value_np = property_dict[(property_key[0], property_key[1], property_key[2], property_key[3], property_key[4], property_key[5].replace('u', 'v'), property_key[6], property_key[7], property_key[8].replace('U', 'V'), property_key[9])]
                            wind_speed_np = np.sqrt(np.power(u_value_np, 2) + np.power(v_value_np, 2))
                            wind_direction_np = np.degrees(np.arctan2(v_value_np, u_value_np))
                            wind_direction_np = np.array([value + 360.0 if value < 0 else value for value in wind_direction_np])
                            if out_file in out_name_dict:
                                out_name_dict[out_file] = out_name_dict[out_file] + [re.sub(r'U wind component', 'wind speed [m/s]', property_key[8]), re.sub(r'U wind component', 'wind direction [degree]', property_key[8])]
                                out_data_dict[out_file] = out_data_dict[out_file] + [pa.array(np.array(wind_speed_np, dtype=property_key[9])), pa.array(np.array(wind_direction_np, dtype=property_key[9]))]
                            else:
                                out_name_dict[out_file] = [re.sub(r'U wind component', 'wind speed [m/s]', property_key[8]), re.sub(r'U wind component', 'wind direction [degree]', property_key[8])]
                                out_data_dict[out_file] = [pa.array(np.array(wind_speed_np, dtype=property_key[9])), pa.array(np.array(wind_direction_np, dtype=property_key[9]))]
                        elif not re.match(r'^V wind component$', property_key[8]):
                            if out_file in out_name_dict:
                                out_name_dict[out_file] = out_name_dict[out_file] + [property_key[8]]
                                out_data_dict[out_file] = out_data_dict[out_file] + [pa.array(np.array(property_dict[property_key], dtype=property_key[9]))]
                            else:
                                out_name_dict[out_file] = [property_key[8]]
                                out_data_dict[out_file] = [pa.array(np.array(property_dict[property_key], dtype=property_key[9]))]
                    for out_file_key in out_name_dict.keys():
                        batch = pa.record_batch(out_data_dict[out_file_key], names=out_name_dict[out_file_key])
                        with open(out_file_key, 'bw') as out_f:
                            ipc_writer = pa.ipc.new_file(out_f, batch.schema, options=pa.ipc.IpcWriteOptions(compression='zstd'))
                            ipc_writer.write_batch(batch)
                            ipc_writer.close()
                            print(out_file_key, file=out_list_file)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--write_location", action='store_true')
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    config = pkg_resources.resource_filename(__name__, 'conf_grib_to_arrow.csv')
    if not re.match(r'^[A-Z][A-Z0-9]{3}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z][A-Z0-9]{3}$).', file=sys.stderr)
        sys.exit(errno)
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
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, conf_df, args.write_location, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
