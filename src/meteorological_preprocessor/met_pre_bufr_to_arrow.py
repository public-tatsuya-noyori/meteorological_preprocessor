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
from datetime import datetime, timezone
from pyarrow import csv
from eccodes import *

def convert_to_arrow(in_file_list, out_dir, cat, subcat, out_list_file, conf_loc_time_list, conf_prop_list, debug):
    warno = 189
    out_arrows = []
    now = datetime.utcnow()
    out_file_date_hour_arrow_list = [str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2), '.arrow']
    out_file_date_hour_arrow = ''.join(out_file_date_hour_arrow_list)
    for cccc in list(set([re.sub('^.*/', '', re.sub('/bufr/' + cat + '/' + subcat + '/' +'.*$', '', in_file)) for in_file in in_file_list if re.match(r'^.*/bufr/' + cat + '/' + subcat + '/.*$', in_file)])):
        loc_time_dict = {}
        prop_dict = {}
        datatype_dict = {}
        is_millisecond = False
        for in_file in in_file_list:
            if not re.match(r'^.*/' + cccc + '/bufr/' + cat + '/' + subcat + '/.*$', in_file):
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
            with open(in_file, 'r') as in_file_stream:
                if debug:
                    print('Debug', ':', in_file, file=sys.stderr)
                while True:
                    bufr = codes_bufr_new_from_file(in_file_stream)
                    if bufr is None:
                        break
                    tmp_loc_time_dict = {}
                    try:
                        codes_set(bufr, 'unpack', 1)
                        none_list = []
                        is_loc_time = True
                        for conf_row in conf_loc_time_list:
                            values = codes_get_array(bufr, conf_row.key)
                            if conf_row.slide > -1 and conf_row.step > 0:
                                values = np.array(values)[conf_row.slide::conf_row.step]
                            if len(values) > 0:
                                if type(values[0]) == str:
                                    values = [value.lstrip().rstrip() for value in values]
                                else:
                                    if conf_row.name == 'location':
                                        values = [None if value < conf_row.min or value > conf_row.max else value for value in values]
                                    elif conf_row.name == 'datetime':
                                        if conf_row.key == 'year':
                                            values = [None if value < conf_row.min or value > conf_row.max else str(value).zfill(4) for value in values]
                                        elif conf_row.key == 'millisecond':
                                            values = [None if value < conf_row.min or value > conf_row.max else '.' + str(value).zfill(4) for value in values]
                                            is_millisecond = True
                                        else:
                                            values = [None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in values]
                                    else:
                                        values = [None if value < conf_row.min or value > conf_row.max else value for value in values]
                                    if conf_row.name == 'longitude [degree]':
                                        values = [conf_row.max if value == conf_row.min else value for value in values]
                                tmp_loc_time_dict[conf_row.key] = values
                                if conf_row.name == 'location' or conf_row.name == 'datetime':
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
                        break
                    tmp_prop_dict = {}
                    try:
                        for conf_row in conf_prop_list:
                            values = codes_get_array(bufr, conf_row.key)
                            if conf_row.slide > -1 and conf_row.step > 0:
                                values = np.array(values)[conf_row.slide::conf_row.step]
                            if len(values) > 0:
                                if type(values[0]) == str:
                                    values = [value.lstrip().rstrip() for value in values]
                                else:
                                    values = [None if value < conf_row.min or value > conf_row.max else value for value in values]
                                tmp_prop_dict[conf_row.name] = values
                    except CodesInternalError as err:
                        print('Warning', warno, ':', 'CodesInternalError is happend at tmp_prop_dict in', in_file, file=sys.stderr)
                        break
                    codes_release(bufr)
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
                            datatype_dict[conf_row.name] = conf_row.datatype
                            if conf_row.name in conf_loc_time_name_keys_dict:
                                conf_loc_time_name_keys_dict[conf_row.name] = conf_loc_time_name_keys_dict[conf_row.name] + [conf_row.key]
                            else:
                                conf_loc_time_name_keys_dict[conf_row.name] = [conf_row.key]
                            if conf_row.name == 'location':
                                tmp_loc_id_list = tmp_loc_time_dict[conf_row.key]
                                del_counter = 0
                                for none_index in none_index_list:
                                    del tmp_loc_id_list[none_index - del_counter]
                                    del_counter += 1
                                if len(tmp_loc_id_np) > 0:
                                    if conf_row.multiply != 0:
                                        tmp_loc_id_np = tmp_loc_id_np + conf_row.multiply * np.array(tmp_loc_id_list, dtype=object)
                                    else:
                                        tmp_loc_id_np = tmp_loc_id_np + '_' + np.array(tmp_loc_id_list, dtype=object)
                                else:
                                    if conf_row.multiply != 0:
                                        tmp_loc_id_np = conf_row.multiply * np.array(tmp_loc_id_list, dtype=object)
                                    else:
                                        tmp_loc_id_np = np.array(tmp_loc_id_list, dtype=object)
                            elif conf_row.name == 'datetime':
                                tmp_datetime_list = tmp_loc_time_dict[conf_row.key]
                                del_counter = 0
                                for none_index in none_index_list:
                                    del tmp_datetime_list[none_index - del_counter]
                                    del_counter += 1
                                if len(tmp_datetime_np) > 0:
                                    tmp_datetime_np = tmp_datetime_np + np.array(tmp_datetime_list, dtype=object)
                                else:
                                    tmp_datetime_np = np.array(tmp_datetime_list, dtype=object)
                                if conf_row.key == 'second':
                                    is_second = True
                                if conf_row.key == 'millisecond':
                                    is_millisecond = True
                            elif conf_row.name == 'latitude [degree]':
                                tmp_latitude_list = tmp_loc_time_dict[conf_row.key]
                                del_counter = 0
                                for none_index in none_index_list:
                                    del tmp_latitude_list[none_index - del_counter]
                                    del_counter += 1
                                if len(tmp_latitude_np) > 0:
                                    tmp_latitude_np = tmp_latitude_np + np.array(tmp_latitude_list)
                                else:
                                    tmp_latitude_np = np.array(tmp_latitude_list)
                            elif conf_row.name == 'longitude [degree]':
                                tmp_longitude_list = tmp_loc_time_dict[conf_row.key]
                                del_counter = 0
                                for none_index in none_index_list:
                                    del tmp_longitude_list[none_index - del_counter]
                                    del_counter += 1
                                if len(tmp_longitude_np) > 0:
                                    tmp_longitude_np = tmp_longitude_np + np.array(tmp_longitude_list)
                                else:
                                    tmp_longitude_np = np.array(tmp_longitude_list)
                            elif conf_row.name == 'height':
                                tmp_height_list = tmp_loc_time_dict[conf_row.key]
                                del_counter = 0
                                for none_index in none_index_list:
                                    del tmp_height_list[none_index - del_counter]
                                    del_counter += 1
                                if len(tmp_height_np) > 0:
                                    tmp_height_np = tmp_height_np + np.array(tmp_height_list)
                                else:
                                    tmp_height_np = np.array(tmp_height_list)
                        if len(tmp_loc_id_np) > 0 and len(tmp_datetime_np) > 0:
                            if 'location' in loc_time_dict:
                                loc_id_list = loc_time_dict['location']
                                loc_time_dict['location'] = loc_id_list + tmp_loc_id_np.tolist()
                            elif 'location' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['location'] = tmp_loc_id_np.tolist()
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
                            if 'latitude [degree]' in loc_time_dict:
                                latitude_list = loc_time_dict['latitude [degree]']
                                loc_time_dict['latitude [degree]'] = latitude_list + tmp_latitude_np.tolist()
                            elif 'latitude [degree]' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['latitude [degree]'] = tmp_latitude_np.tolist()
                            if 'longitude [degree]' in loc_time_dict:
                                longitude_list = loc_time_dict['longitude [degree]']
                                loc_time_dict['longitude [degree]'] = longitude_list + tmp_longitude_np.tolist()
                            elif 'longitude [degree]' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['longitude [degree]'] = tmp_longitude_np.tolist()
                            if 'height' in loc_time_dict:
                                height_list = loc_time_dict['height']
                                loc_time_dict['height'] = height_list + tmp_height_np.tolist()
                            elif 'height' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['height'] = tmp_height_np.tolist()
                            for conf_row in conf_prop_list:
                                datatype_dict[conf_row.name] = conf_row.datatype
                                tmp_prop_list = tmp_prop_dict[conf_row.name]
                                del_counter = 0
                                for none_index in none_index_list:
                                    del tmp_prop_list[none_index - del_counter]
                                    del_counter += 1
                                prop_list = []
                                if conf_row.name in prop_dict:
                                    prop_list = prop_dict[conf_row.name]
                                    prop_dict[conf_row.name] = prop_list + tmp_prop_list
                                else:
                                    prop_dict[conf_row.name] = tmp_prop_list
        if 'datetime' in loc_time_dict:
            id_list = [id_num for id_num in range(0, len(loc_time_dict['datetime']))]
            loc_time_data = [pa.array(id_list, 'uint32')]
            name_list = ['id']
            for key in loc_time_dict.keys():
                if key == 'datetime':
                    if is_millisecond:
                        loc_time_data.append(pa.array(loc_time_dict[key], pa.timestamp('ms', tz='utc')))
                    else:
                        loc_time_data.append(pa.array(loc_time_dict[key], pa.timestamp('s', tz='utc')))
                else:
                    loc_time_data.append(pa.array(loc_time_dict[key], datatype_dict[key]))
                name_list.append(key)
            datetime_directory_list = [str(datetime.year).zfill(4) + str(datetime.month).zfill(2) + str(datetime.day).zfill(2) + str(datetime.hour).zfill(2) for datetime in loc_time_dict['datetime']]
            for datetime_directory in set(datetime_directory_list):
                datetime_index_list = [index for index, value in enumerate(loc_time_dict['datetime']) if value.year == int(datetime_directory[0:4]) and value.month == int(datetime_directory[4:6]) and value.day == int(datetime_directory[6:8]) and value.hour == int(datetime_directory[8:10])]
                tmp_loc_time_data = [loc_time.take(pa.array(datetime_index_list)) for loc_time in loc_time_data]
                if len(tmp_loc_time_data) > 0:
                    out_directory_list = [out_dir, cccc, 'bufr_to_arrow', cat, subcat, 'location_datetime']
                    out_directory = '/'.join(out_directory_list)
                    os.makedirs(out_directory + '/' + datetime_directory, exist_ok=True)
                    out_file_list = [out_directory, datetime_directory, out_file_date_hour_arrow]
                    out_file = '/'.join(out_file_list)
                    with open(out_file, 'bw') as out_f:
                        loc_time_batch = pa.record_batch(tmp_loc_time_data, names=name_list)
                        writer = pa.ipc.new_file(out_f, loc_time_batch.schema)
                        writer.write_batch(loc_time_batch)
                        writer.close()
                        print(out_file, file=out_list_file)
                for key in prop_dict.keys():
                    datetime_id_list = pa.array(id_list, 'uint32').take(pa.array(datetime_index_list))
                    datetime_prop_data = pa.array(prop_dict[key], datatype_dict[key]).take(pa.array(datetime_index_list))
                    value_index_list = [index for index, value in enumerate(datetime_prop_data.tolist()) if value != None]
                    if len(value_index_list) > 0:
                        prop_data = []
                        prop_data.append(datetime_id_list.take(pa.array(value_index_list)))
                        prop_data.append(datetime_prop_data.take(pa.array(value_index_list)))
                        out_directory_list = [out_dir, cccc, 'bufr_to_arrow', cat, subcat, key.split('[')[0].strip().replace(' ', '_')]
                        out_directory = '/'.join(out_directory_list)
                        os.makedirs(out_directory + '/' + datetime_directory, exist_ok=True)
                        out_file_list = [out_directory, datetime_directory, out_file_date_hour_arrow]
                        out_file = '/'.join(out_file_list)
                        with open(out_file, 'bw') as out_f:
                            prop_batch = pa.record_batch(prop_data, names=['id', key])
                            writer = pa.ipc.new_file(out_f, prop_batch.schema)
                            writer.write_batch(prop_batch)
                            writer.close()
                            print(out_file, file=out_list_file)

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
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        conf_location_time_list = list(csv.read_csv(config_location_time).to_pandas().itertuples())
        conf_properties_list = list(csv.read_csv(config_properties).to_pandas().itertuples())
        convert_to_arrow(input_file_list, args.output_directory, args.category, args.subcategory, args.output_list_file, conf_location_time_list, conf_properties_list, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
