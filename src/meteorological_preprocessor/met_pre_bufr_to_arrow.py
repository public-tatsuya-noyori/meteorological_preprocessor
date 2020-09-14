#!/usr/bin/env python3
import argparse
import math
import os
import pkg_resources
import pyarrow as pa
import pandas as pd
import re
import sys
import traceback
import numpy as np
from datetime import datetime, timezone
from pyarrow import csv
from eccodes import *

def convert_to_arrow(my_cccc, in_file_list, out_dir, cat, subcat, out_list_file, conf_loc_time_list, conf_prop_list, debug):
    warno = 189
    out_arrows = []
    now = datetime.utcnow()
    out_file_date_hour_arrow_list = ['C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2), '.arrow']
    out_file_date_hour_arrow = ''.join(out_file_date_hour_arrow_list)
    for cccc in list(set([re.sub('^.*/', '', re.sub('/bufr/' + cat + '/' + subcat + '/' +'.*$', '', in_file)) for in_file in in_file_list if re.match(r'^.*/bufr/' + cat + '/' + subcat + '/.*$', in_file)])):
        loc_time_dict = {}
        prop_dict = {}
        datatype_dict = {}
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
                        none_np = np.array([])
                        datetime_tail = ''
                        is_loc_time = True
                        for conf_row in conf_loc_time_list:
                            values = codes_get_array(bufr, conf_row.key)
                            if conf_row.name == 'descriptor':
                                if len(values) == 0:
                                    is_loc_time = False
                                    print('Info', ':', 'not found descriptor.', in_file, file=sys.stderr)
                                    break
                                if values[0] < conf_row.min or values[0] > conf_row.max:
                                    is_loc_time = False
                                    print('Info', ':', values[0], 'is not in range of descriptor.', in_file, file=sys.stderr)
                                    break
                            if conf_row.slide > -1 and conf_row.step > 0:
                                values = np.array(values)[conf_row.slide::conf_row.step]
                            if len(values) > 0:
                                if values[0] == str:
                                    values = np.array([value.lstrip().rstrip() for value in values], dtype=object)
                                else:
                                    values_df = pd.DataFrame(values)
                                    not_nan_values_df = values_df.where((pd.notnull(values_df)), None)
                                    values = not_nan_values_df.to_numpy()
                                    if conf_row.name == 'datetime':
                                        datetime_tail = conf_row.key
                                        if conf_row.key == 'year':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values * 10000000000000)
                                        elif conf_row.key == 'month':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values * 100000000000)
                                        elif conf_row.key == 'day':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values * 1000000000)
                                        elif conf_row.key == 'hour':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values * 10000000)
                                        elif conf_row.key == 'minute':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values * 100000)
                                        elif conf_row.key == 'second':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values * 1000)
                                        elif conf_row.key == 'millisecond':
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values)
                                    else:
                                        values = np.where((values < conf_row.min) | (values > conf_row.max), None, values)
                                    if conf_row.name == 'longitude [degree]':
                                        values = np.where(values == conf_row.min, conf_row.max, values)
                                tmp_loc_time_dict[conf_row.key] = values
                                if conf_row.name == 'location' or conf_row.name == 'datetime':
                                    tmp_none_np = np.array([False if value == None else True for value in values])
                                    if len(none_np) > 0:
                                        none_np = none_np * tmp_none_np
                                    else:
                                        none_np = tmp_none_np
                            else:
                                is_loc_time = False
                                break
                        if not is_loc_time or not True in none_np.tolist():
                            codes_release(bufr)
                            break
                        tmp_loc_time_dict['none'] = none_np
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
                                if values[0] == str:
                                    values = np.array([value.lstrip().rstrip() for value in values], dtype=object)
                                else:
                                    values_df = pd.DataFrame(values)
                                    not_nan_values_df = values_df.where((pd.notnull(values_df)), None)
                                    values = not_nan_values_df.to_numpy()
                                    values = np.where((values < conf_row.min) | (values > conf_row.max), None, values)
                                tmp_prop_dict[conf_row.name] = values
                    except CodesInternalError as err:
                        print('Warning', warno, ':', 'CodesInternalError is happend at tmp_prop_dict in', in_file, file=sys.stderr)
                        break
                    codes_release(bufr)
                    if len(tmp_prop_dict) == 0 or len(tmp_loc_time_dict) == 0:
                        break
                    else:
                        loc_time_index_np = np.array([index for index, value in enumerate(tmp_loc_time_dict['none']) if value == True])
                        message_loc_id_np = np.array([])
                        message_datetime_np = np.array([])
                        message_latitude_np = np.array([])
                        message_longitude_np = np.array([])
                        message_height_np = np.array([])
                        conf_loc_time_name_keys_dict = {}
                        for conf_row in conf_loc_time_list:
                            datatype_dict[conf_row.name] = conf_row.datatype
                            if conf_row.name in conf_loc_time_name_keys_dict:
                                conf_loc_time_name_keys_dict[conf_row.name] = conf_loc_time_name_keys_dict[conf_row.name] + [conf_row.key]
                            else:
                                conf_loc_time_name_keys_dict[conf_row.name] = [conf_row.key]
                            if conf_row.name == 'location':
                                tmp_loc_id_np = tmp_loc_time_dict[conf_row.key]
                                if len(loc_time_index_np) > 0:
                                    tmp_loc_id_np = tmp_loc_id_np[loc_time_index_np]
                                if len(tmp_loc_id_np) > 0:
                                    if len(message_loc_id_np) > 0:
                                        if conf_row.multiply != 0:
                                            message_loc_id_np = message_loc_id_np + conf_row.multiply * tmp_loc_id_np
                                        else:
                                            message_loc_id_np = message_loc_id_np + '_' + tmp_loc_id_np
                                    else:
                                        if conf_row.multiply != 0:
                                            message_loc_id_np = conf_row.multiply * tmp_loc_id_np
                                        else:
                                            message_loc_id_np = tmp_loc_id_np
                            elif conf_row.name == 'datetime':
                                tmp_datetime_np = tmp_loc_time_dict[conf_row.key]
                                if len(loc_time_index_np) > 0:
                                    tmp_datetime_np = tmp_datetime_np[loc_time_index_np]
                                if len(tmp_datetime_np) > 0:
                                    if len(message_datetime_np) > 0:
                                        message_datetime_np = message_datetime_np + tmp_datetime_np
                                    else:
                                        message_datetime_np = tmp_datetime_np
                            elif conf_row.name == 'latitude [degree]':
                                tmp_latitude_np = tmp_loc_time_dict[conf_row.key]
                                if len(loc_time_index_np) > 0:
                                    tmp_latitude_np = tmp_latitude_np[loc_time_index_np]
                                if len(tmp_latitude_np) > 0:
                                    if len(message_latitude_np) > 0:
                                        message_latitude_np = message_latitude_np + tmp_latitude_np
                                    else:
                                        message_latitude_np = tmp_latitude_np
                            elif conf_row.name == 'longitude [degree]':
                                tmp_longitude_np = tmp_loc_time_dict[conf_row.key]
                                if len(loc_time_index_np) > 0:
                                    tmp_longitude_np = tmp_longitude_np[loc_time_index_np]
                                if len(tmp_longitude_np) > 0:
                                    if len(message_longitude_np) > 0:
                                        message_longitude_np = message_longitude_np + tmp_longitude_np
                                    else:
                                        message_longitude_np = tmp_longitude_np
                            elif conf_row.name == 'height':
                                tmp_height_np = tmp_loc_time_dict[conf_row.key]
                                if len(loc_time_index_np) > 0:
                                    tmp_height_np = tmp_height_np[loc_time_index_np]
                                if len(tmp_height_np) > 0:
                                    if len(message_height_np) > 0:
                                        message_height_np = message_height_np + tmp_height_np
                                    else:
                                        message_height_np = tmp_height_np
                        if len(message_loc_id_np) > 0 and len(message_datetime_np) > 0:
                            if 'location' in loc_time_dict:
                                loc_time_dict['location'] = np.concatenate([loc_time_dict['location'], message_loc_id_np])
                            elif 'location' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['location'] = message_loc_id_np
                            tmp_datetime_list = []
                            if datetime_tail == 'millisecond':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), int(str(dt)[4:6]), int(str(dt)[6:8]), int(str(dt)[8:10]), int(str(dt)[10:12]), int(str(dt)[12:14]), int(str(dt)[15:]), tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            elif datetime_tail == 'second':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), int(str(dt)[4:6]), int(str(dt)[6:8]), int(str(dt)[8:10]), int(str(dt)[10:12]), int(str(dt)[12:14]), 0, tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            elif datetime_tail == 'minute':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), int(str(dt)[4:6]), int(str(dt)[6:8]), int(str(dt)[8:10]), int(str(dt)[10:12]), 0, 0, tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            elif datetime_tail == 'hour':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), int(str(dt)[4:6]), int(str(dt)[6:8]), int(str(dt)[8:10]), 0, 0, 0, tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            elif datetime_tail == 'day':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), int(str(dt)[4:6]), int(str(dt)[6:8]), 0, 0, 0, 0, tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            elif datetime_tail == 'month':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), int(str(dt)[4:6]), 0, 0, 0, 0, 0, tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            elif datetime_tail == 'year':
                                tmp_datetime_list = [datetime(int(str(dt)[0:4]), 0, 0, 0, 0, 0, 0, tzinfo=timezone.utc) for dt in message_datetime_np.flatten()]
                            if 'datetime' in loc_time_dict:
                                loc_time_dict['datetime'] = loc_time_dict['datetime'] + tmp_datetime_list
                            elif 'datetime' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['datetime'] = tmp_datetime_list
                            if 'latitude [degree]' in loc_time_dict:
                                loc_time_dict['latitude [degree]'] = np.concatenate([loc_time_dict['latitude [degree]'], message_latitude_np])
                            elif 'latitude [degree]' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['latitude [degree]'] = message_latitude_np
                            if 'longitude [degree]' in loc_time_dict:
                                loc_time_dict['longitude [degree]'] = np.concatenate([loc_time_dict['longitude [degree]'], message_longitude_np])
                            elif 'longitude [degree]' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['longitude [degree]'] = message_longitude_np
                            if 'height' in loc_time_dict:
                                loc_time_dict['height'] = np.concatenate([loc_time_dict['height'], message_height_np])
                            elif 'height' in conf_loc_time_name_keys_dict.keys():
                                loc_time_dict['height'] = message_height_np
                            for conf_row in conf_prop_list:
                                datatype_dict[conf_row.name] = conf_row.datatype
                                if conf_row.name in tmp_prop_dict:
                                    tmp_prop_np = tmp_prop_dict[conf_row.name]
                                    if len(loc_time_index_np) > 0:
                                        tmp_prop_np = tmp_prop_np[loc_time_index_np]
                                    if len(tmp_prop_np) > 0:
                                        if conf_row.name in prop_dict:
                                            prop_dict[conf_row.name] = np.concatenate([prop_dict[conf_row.name], tmp_prop_np])
                                        else:
                                            prop_dict[conf_row.name] = tmp_prop_np
        if 'datetime' in loc_time_dict:
            id_list = [id_num for id_num in range(0, len(loc_time_dict['datetime']))]
            loc_time_data = [pa.array(id_list, 'uint32')]
            name_list = ['id']
            for key in loc_time_dict.keys():
                if key == 'datetime':
                    loc_time_data.append(pa.array(loc_time_dict[key], pa.timestamp('ms', tz='utc')))
                else:
                    loc_time_data.append(pa.array(loc_time_dict[key].flatten(), datatype_dict[key]))
                name_list.append(key)
            datetime_directory_list = [str(datetime.year).zfill(4) + str(datetime.month).zfill(2) + str(datetime.day).zfill(2) + str(datetime.hour).zfill(2) + str(datetime.minute).zfill(2)[0:1] + "0" for datetime in loc_time_dict['datetime']]
            for datetime_directory in set(datetime_directory_list):
                datetime_index_list = [index for index, value in enumerate(loc_time_dict['datetime']) if value.year == int(datetime_directory[0:4]) and value.month == int(datetime_directory[4:6]) and value.day == int(datetime_directory[6:8]) and value.hour == int(datetime_directory[8:10]) and value.minute / 10 == int(datetime_directory[10:11])]
                if len(datetime_index_list) > 0:
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
                        datetime_prop_data = prop_dict[key][datetime_index_list]
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
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('category', type=str, metavar='category')
    parser.add_argument('subcategory', type=str, metavar='subcategory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    config_location_time = pkg_resources.resource_filename(__name__, 'conf_bufr_to_arrow_' + args.category + '_' + args.subcategory + '_location_time.csv')
    config_properties = pkg_resources.resource_filename(__name__, 'conf_bufr_to_arrow_' + args.category + '_' + args.subcategory + '_properties.csv')
    if not re.match(r'^[A-Z]{4}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z]{4}$).', file=sys.stderr)
        sys.exit(errno)
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
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.category, args.subcategory, args.output_list_file, conf_location_time_list, conf_properties_list, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
