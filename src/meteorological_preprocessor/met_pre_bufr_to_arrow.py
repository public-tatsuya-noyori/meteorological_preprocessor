#!/usr/bin/env python3
import argparse
import math
import os
import pkg_resources
import pyarrow as pa
import re
import sys
import traceback
import numpy as np
from datetime import datetime, timedelta, timezone
from pyarrow import csv
from eccodes import *

def convert_to_arrow(in_list_file, out_dir, cat, subcat, out_list_file, conf_loc_time_list, conf_prop_list, debug):
    warno = 189
    out_arrows = []
    with open(in_list_file, 'r') as in_list_file_stream:
        loc_time_dict = {}
        prop_dict = {}
        for in_file in in_list_file_stream.readlines():
            in_file = in_file.rstrip('\n')
            matched = re.search('/bufr_'+ cat + '_' + subcat + '/', in_file)
            if not matched:
                print('Debug', ':', in_file, 'is not', '/bufr_'+ cat + '_' + subcat + '/.', file=sys.stderr)
                continue
            elif not os.access(in_file, os.F_OK):
                print('Warning', warno, ':', in_file, 'does not exist.', file=sys.stderr)
                continue
            elif not os.path.isfile(in_file):
                print('Warning', warno, ':', in_file, 'is not file.', file=sys.stderr)
                continue
            elif not os.access(in_file, os.R_OK):
                print('Warning', warno, ':', in_file, 'is not readable.', file=sys.stderr)
                continue
            in_file_stream = open(in_file, 'r')
            if debug:
                print('Debug', ':', in_file, file=sys.stderr)
            while True:
                bufr = codes_bufr_new_from_file(in_file_stream)
                if bufr is None:
                    break
                try:
                    codes_set(bufr, 'unpack', 1)
                except CodesInternalError as err:
                    print('Warning', warno, ':', 'CodesInternalError is happend at unpack in', in_file, file=sys.stderr)
                    codes_release(bufr)
                    break
                tmp_loc_time_dict = {}
                try:
                    none_list = []
                    is_loc_time = True
                    for conf_row in conf_loc_time_list:
                        values = codes_get_array(bufr, conf_row.key)
                        if len(values) > 0:
                            if type(values[0]) == str:
                                values = [value.lstrip().rstrip() for value in values]
                            else:
                                if conf_row.name == 'location_id':
                                    values = [None if value < conf_row.min or value > conf_row.max else str(value) for value in codes_get_array(bufr, conf_row.key)]
                                elif conf_row.name == 'datetime':
                                    if conf_row.key == 'year':
                                        values = [None if value < conf_row.min or value > conf_row.max else str(value).zfill(4) for value in codes_get_array(bufr, conf_row.key)]
                                    elif conf_row.key == 'millisecond':
                                        values = [None if value < conf_row.min or value > conf_row.max else '.' + str(value).zfill(4) for value in codes_get_array(bufr, conf_row.key)]
                                    else:
                                        values = [None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in codes_get_array(bufr, conf_row.key)]

                                else:
                                    values = [None if value < conf_row.min or value > conf_row.max else value for value in codes_get_array(bufr, conf_row.key)]
                            tmp_loc_time_dict[conf_row.key] = values
                            if conf_row.name == 'location_id' or conf_row.name == 'datetime':
                                tmp_none_list = [False if value == None else True for value in values]
                                if len(none_list) > 0:
                                    none_list = none_list * np.array(tmp_none_list)
                                else:
                                    none_list = np.array(tmp_none_list)
                        else:
                            is_loc_time = False
                            break
                    if not is_loc_time:
                        codes_release(bufr)
                        break
                    tmp_loc_time_dict['none'] = none_list
                except CodesInternalError as err:
                    print('Warning', warno, ':', 'CodesInternalError is happend at tmp_loc_time_dict in', in_file, file=sys.stderr)
                    codes_release(bufr)
                    break
                tmp_prop_dict = {}
                try:
                    for conf_row in conf_prop_list:
                        values = codes_get_array(bufr, conf_row.key)
                        if len(values) > 0:
                            if type(values[0]) == str:
                                values = [value.lstrip().rstrip() for value in values]
                            else:
                                values = [None if value < conf_row.min or value > conf_row.max else value for value in codes_get_array(bufr, conf_row.key)]
                            tmp_prop_dict[conf_row.key] = values
                except CodesInternalError as err:
                    print('Warning', warno, ':', 'CodesInternalError is happend at tmp_prop_dict in', in_file, file=sys.stderr)
                    codes_release(bufr)
                    break
                if len(tmp_prop_dict) > 0 and len(tmp_loc_time_dict) > 0:
                    none_index_list = [index for index, value in enumerate(tmp_loc_time_dict['none']) if value == False]
                    tmp_loc_id_np = np.array([])
                    tmp_datetime_np = np.array([])
                    tmp_latitude_np = np.array([])
                    tmp_longitude_np = np.array([])
                    tmp_height_np = np.array([])
                    is_second = False
                    is_millisecond = False
                    conf_loc_time_name_keys_dict = {}
                    for conf_row in conf_loc_time_list:
                        if conf_row.name in conf_loc_time_name_keys_dict:
                            conf_loc_time_name_keys_dict[conf_row.name] = conf_loc_time_name_keys_dict[conf_row.name] + [conf_row.key]
                        else:
                            conf_loc_time_name_keys_dict[conf_row.name] = [conf_row.key]
                        if conf_row.name == 'location_id':
                            tmp_loc_id_list = tmp_loc_time_dict[conf_row.key]
                            for none_index in none_index_list:
                                del tmp_loc_id_list[none_index]
                            if len(tmp_loc_id_np) > 0:
                                tmp_loc_id_np = tmp_loc_id_np + '_' + np.array(tmp_loc_id_list, dtype=object)
                            else:
                                tmp_loc_id_np = np.array(tmp_loc_id_list, dtype=object)
                        elif conf_row.name == 'datetime':
                            tmp_datetime_list = tmp_loc_time_dict[conf_row.key]
                            for none_index in none_index_list:
                                del tmp_datetime_list[none_index]
                            if len(tmp_datetime_np) > 0:
                                tmp_datetime_np = tmp_datetime_np + np.array(tmp_datetime_list, dtype=object)
                            else:
                                tmp_datetime_np = np.array(tmp_datetime_list, dtype=object)
                            if conf_row.key == 'second':
                                is_second = True
                            if conf_row.key == 'millisecond':
                                is_millisecond = True
                        elif conf_row.name == 'latitude':
                            tmp_latitude_list = tmp_loc_time_dict[conf_row.key]
                            for none_index in none_index_list:
                                del tmp_latitude_list[none_index]
                            if len(tmp_latitude_np) > 0:
                                tmp_latitude_np = tmp_latitude_np + np.array(tmp_latitude_list, dtype=object)
                            else:
                                tmp_latitude_np = np.array(tmp_latitude_list, dtype=object)
                        elif conf_row.name == 'longitude':
                            tmp_longitude_list = tmp_loc_time_dict[conf_row.key]
                            for none_index in none_index_list:
                                del tmp_longitude_list[none_index]
                            if len(tmp_longitude_np) > 0:
                                tmp_longitude_np = tmp_longitude_np + np.array(tmp_longitude_list, dtype=object)
                            else:
                                tmp_longitude_np = np.array(tmp_longitude_list, dtype=object)
                        elif conf_row.name == 'height':
                            tmp_height_list = tmp_loc_time_dict[conf_row.key]
                            for none_index in none_index_list:
                                del tmp_height_list[none_index]
                            if len(tmp_height_np) > 0:
                                tmp_height_np = tmp_height_np + np.array(tmp_height_list, dtype=object)
                            else:
                                tmp_height_np = np.array(tmp_height_list, dtype=object)
                    if len(tmp_loc_id_np) > 0 and len(tmp_datetime_np) > 0:
                        if 'location_id' in loc_time_dict:
                            loc_id_list = loc_time_dict['location_id']
                            loc_time_dict['location_id'] = loc_id_list + tmp_loc_id_np.tolist()
                        elif 'location_id' in conf_loc_time_name_keys_dict.keys():
                            loc_time_dict['location_id'] = tmp_loc_id_np.tolist()
                        tmp_datetime_list = []
                        if not is_second:
                            tmp_datetime_list = [datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), 0, 0, tzinfo=timezone.utc) for dt_str in tmp_datetime_np.tolist()]
                        elif not is_millisecond:
                            tmp_datetime_list = [datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), int(dt_str[12:14]), 0, tzinfo=timezone.utc) for dt_str in tmp_datetime_np.tolist()]
                        else:
                            tmp_datetime_list = [datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), int(dt_str[12:14]), int(dt_str[15:]), tzinfo=timezone.utc) for dt_str in tmp_datetime_np.tolist()]
                        if 'datetime' in loc_time_dict:
                            datetime_list = loc_time_dict['datetime']
                            loc_time_dict['datetime'] = datetime_list + tmp_datetime_list
                        elif 'datetime' in conf_loc_time_name_keys_dict.keys():
                            loc_time_dict['datetime'] = tmp_datetime_list
                        if 'latitude' in loc_time_dict:
                            latitude_list = loc_time_dict['latitude']
                            loc_time_dict['latitude'] = latitude_list + tmp_latitude_np.tolist()
                        elif 'latitude' in conf_loc_time_name_keys_dict.keys():
                            loc_time_dict['latitude'] = tmp_latitude_np.tolist()
                        if 'longitude' in loc_time_dict:
                            longitude_list = loc_time_dict['longitude']
                            loc_time_dict['longitude'] = longitude_list + tmp_longitude_np.tolist()
                        elif 'longitude' in conf_loc_time_name_keys_dict.keys():
                            loc_time_dict['longitude'] = tmp_longitude_np.tolist()
                        if 'height' in loc_time_dict:
                            height_list = loc_time_dict['height']
                            loc_time_dict['height'] = height_list + tmp_height_np.tolist()
                        elif 'height' in conf_loc_time_name_keys_dict.keys():
                            loc_time_dict['height'] = tmp_height_np.tolist()
                        for conf_row in conf_prop_list:
                            tmp_prop_list = tmp_prop_dict[conf_row.key]
                            del_counter = 0
                            for none_index in none_index_list:
                                del tmp_prop_list[none_index - del_counter]
                                del_counter += 1
                            prop_list = []
                            if conf_row.key in prop_dict:
                                prop_list = prop_dict[conf_row.key]
                                prop_dict[conf_row.key] = prop_list + tmp_prop_list
                            else:
                                prop_dict[conf_row.key] = tmp_prop_list
                codes_release(bufr)
            in_file_stream.close()
        id_list = [id_num for id_num in range(1, len(loc_time_dict['datetime']) + 1)]
        loc_time_data = [pa.array(id_list)]
        name_list = ['id']
        for key in loc_time_dict.keys():
            loc_time_data.append(pa.array(loc_time_dict[key]))
            name_list.append(key)
        loc_time_batch = pa.record_batch(loc_time_data, names=name_list)
        with open('loc_time.arrow', 'bw') as out_f:
            writer = pa.ipc.new_file(out_f, loc_time_batch.schema)
            writer.write_batch(loc_time_batch)
            writer.close()
        for key in prop_dict.keys():
            tmp_prop_list = prop_dict[key]
            tmp_id_list = id_list.copy()
            none_index_list = [index for index, value in enumerate(tmp_prop_list) if value == None]
            del_counter = 0
            for none_index in none_index_list:
                del tmp_prop_list[none_index - del_counter]
                del tmp_id_list[none_index - del_counter]
                del_counter += 1
            prop_data = [pa.array(tmp_id_list), pa.array(tmp_prop_list)]
            prop_batch = pa.record_batch(prop_data, names=['id', key])
            with open(key + '.arrow', 'bw') as out_f:
                writer = pa.ipc.new_file(out_f, prop_batch.schema)
                writer.write_batch(prop_batch)
                writer.close()


def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('category', type=str, metavar='category')
    parser.add_argument('subcategory', type=str, metavar='subcategory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    config_location_time = pkg_resources.resource_filename(__name__, 'conf_bufr_' + args.category + '_' + args.subcategory + '_to_arrow_location_time.csv')
    config_properties = pkg_resources.resource_filename(__name__, 'conf_bufr_' + args.category + '_' + args.subcategory + '_to_arrow_properties.csv')
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(config_location_time, os.F_OK):
        print('Error', errno, ':', config_location_time, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(config_properties, os.F_OK):
        print('Error', errno, ':', config_properties, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(config_location_time):
        print('Error', errno, ':', config_location_time, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(config_properties):
        print('Error', errno, ':', config_properties, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(config_location_time, os.R_OK):
        print('Error', errno, ':', config_location_time, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(config_properties, os.R_OK):
        print('Error', errno, ':', config_properties, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        conf_location_time_list = list(csv.read_csv(config_location_time).to_pandas().itertuples())
        conf_properties_list = list(csv.read_csv(config_properties).to_pandas().itertuples())
        convert_to_arrow(args.input_list_file, args.output_directory, args.category, args.subcategory, args.output_list_file, conf_location_time_list, conf_properties_list, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
