#!/usr/bin/env python3
import argparse
import numpy as np
import os
import pkg_resources
import pyarrow as pa
import re
import sys
import traceback
from datetime import datetime, timezone
from pyarrow import csv
from eccodes import *

def convert_to_arrow(my_cccc, in_file_list, out_dir, out_list_file, conf_df, debug):
    warno = 189
    out_arrows = []
    now = datetime.utcnow()
    create_datetime_directory_list = ['C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)]
    create_datetime_directory = ''.join(create_datetime_directory_list)
    cccc_set = set([re.sub('^.*/', '', re.sub('/bufr/.*$', '', in_file)) for in_file in in_file_list])
    cat_subcat_set = set([re.search(r'^[^/]*/[^/]*/', re.sub('^.*/bufr/', '', in_file)).group().rstrip('/') for in_file in in_file_list])
    for cccc in cccc_set:
        for cat_subcat in cat_subcat_set:
            location_datetime_dict = {}
            property_dict = {}
            datatype_dict = {}
            for in_file in in_file_list:
                match = re.search(r'^.*/' + cccc + '/bufr/' + cat_subcat + '.*$', in_file)
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
                with open(in_file, 'r') as in_file_stream:
                    if debug:
                        print('Debug', ':', in_file, file=sys.stderr)
                    while True:
                        bufr = codes_bufr_new_from_file(in_file_stream)
                        if bufr is None:
                            break
                        bufr_dict = {}
                        datetime_tail = ''
                        try:
                            codes_set(bufr, 'unpack', 1)
                            none_np = np.array([])
                            not_none_np_choice = np.array([])
                            unexpanded_descriptors = codes_get_array(bufr, 'unexpandedDescriptors')
                            descriptor_conf_df = conf_df[conf_df['descriptor'] == unexpanded_descriptors[0]]
                            if len(descriptor_conf_df) == 0:
                                print('Info', ':', unexpanded_descriptors[0], 'not found descriptor.', in_file, file=sys.stderr)
                                break
                            number_of_subsets = codes_get_array(bufr, 'numberOfSubsets')
                            conf_location_datetime_list = list(descriptor_conf_df[(descriptor_conf_df['type'] == 'location') | (descriptor_conf_df['type'] == 'datetime')].itertuples())
                            conf_property_list = list(descriptor_conf_df[(descriptor_conf_df['type'] == 'property')].itertuples())
                            for conf_row in conf_location_datetime_list:
                                values = codes_get_array(bufr, conf_row.key)
                                if conf_row.slide > -1 and conf_row.step > 0:
                                    values = np.array(values)[conf_row.slide::conf_row.step]
                                if len(values) > 0 and len(values) == number_of_subsets:
                                    if values[0] == str:
                                        values = np.array([value.lstrip().rstrip() for value in values], dtype=object)
                                    else:
                                        values = np.where(np.isnan(values), None, values)
                                        if conf_row.type == 'datetime':
                                            datetime_tail = conf_row.key
                                            if conf_row.key == 'year':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(4) for value in values], dtype=object)
                                            elif conf_row.key == 'month':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in values], dtype=object)
                                            elif conf_row.key == 'day':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in values], dtype=object)
                                            elif conf_row.key == 'hour':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in values], dtype=object)
                                            elif conf_row.key == 'minute':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in values], dtype=object)
                                            elif conf_row.key == 'second':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in values], dtype=object)
                                            elif conf_row.key == 'millisecond':
                                                values = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(3) for value in values], dtype=object)
                                        else:
                                            values = np.where((values < conf_row.min) | (values > conf_row.max), None, values)
                                        if conf_row.name == 'longitude [degree]':
                                            values = np.where(values == conf_row.min, conf_row.max, values)
                                    bufr_dict[conf_row.key] = values
                                    if conf_row.type == 'location' or conf_row.type == 'datetime':
                                        if conf_row.condition == 'required':
                                            tmp_none_np = np.array([False if value == None else True for value in values])
                                            if len(none_np) > 0:
                                                none_np = none_np * tmp_none_np
                                            else:
                                                none_np = tmp_none_np
                                        elif conf_row.condition == 'choice':
                                            tmp_not_none_np = np.array([True if value == None else False for value in values])
                                            if len(not_none_np_choice) > 0:
                                                not_none_np_choice = not_none_np_choice * tmp_not_none_np
                                            else:
                                                not_none_np_choice = tmp_not_none_np
                                    if len(not_none_np_choice) > 0:
                                        tmp_none_np_choice = np.array([True if value == False else False for value in not_none_np_choice])
                                        none_np = none_np * tmp_none_np_choice
                                else:
                                    if len(values) != number_of_subsets:
                                        print('Info', ':', len(values), number_of_subsets, 'not equals the number of subsets.', in_file, file=sys.stderr)
                                    bufr_dict = {}
                                    break
                            if len(bufr_dict) == 0 or False in none_np.tolist():
                                codes_release(bufr)
                                break
                            bufr_dict['none'] = none_np
                        except CodesInternalError as err:
                            print('Warning', warno, ':', 'CodesInternalError is happend at bufr_dict in', in_file, file=sys.stderr)
                            break
                        try:
                            for conf_row in conf_property_list:
                                values = codes_get_array(bufr, conf_row.key)
                                if conf_row.slide > -1 and conf_row.step > 0:
                                    values = np.array(values)[conf_row.slide::conf_row.step]
                                if len(values) > 0:
                                    if values[0] == str:
                                        values = np.array([value.lstrip().rstrip() for value in values], dtype=object)
                                    else:
                                        values = np.where(np.isnan(values), None, values)
                                        values = np.where((values < conf_row.min) | (values > conf_row.max), None, values)
                                    bufr_dict[conf_row.key] = values
                        except CodesInternalError as err:
                            print('Warning', warno, ':', 'CodesInternalError is happend at bufr_dict in', in_file, file=sys.stderr)
                            break
                        codes_release(bufr)
                        location_datetime_index_np = np.array([index for index, value in enumerate(bufr_dict['none']) if value == True])
                        if len(location_datetime_index_np) > 0:
                            message_np = np.array([])
                            pre_conf_row_name = ''
                            for conf_row in conf_location_datetime_list:
                                if conf_row.name != pre_conf_row_name:
                                    datatype_dict[conf_row.name] = conf_row.datatype
                                    if len(message_np) > 0 and len(pre_conf_row_name) > 0:
                                        if pre_conf_row_name in location_datetime_dict:
                                            location_datetime_dict[pre_conf_row_name] = np.concatenate([location_datetime_dict[pre_conf_row_name], message_np])
                                        else:
                                            location_datetime_dict[pre_conf_row_name] = message_np
                                        message_np = np.array([])
                                tmp_message_np = bufr_dict[conf_row.key]
                                if len(tmp_message_np) > 0:
                                    tmp_message_np = tmp_message_np[location_datetime_index_np]
                                    if len(tmp_message_np) > 0:
                                        if len(message_np) > 0:
                                            if conf_row.multiply != 0:
                                                message_np = message_np + conf_row.multiply * tmp_message_np
                                            else:
                                                message_np = message_np + tmp_message_np
                                        else:
                                            if conf_row.multiply != 0:
                                                message_np = conf_row.multiply * tmp_message_np
                                            else:
                                                message_np = tmp_message_np
                                pre_conf_row_name = conf_row.name
                            if len(message_np) > 0 and len(pre_conf_row_name) > 0:
                                if pre_conf_row_name in location_datetime_dict:
                                    location_datetime_dict[pre_conf_row_name] = np.concatenate([location_datetime_dict[pre_conf_row_name], message_np])
                                else:
                                    location_datetime_dict[pre_conf_row_name] = message_np
                            message_np = np.array([])
                            pre_conf_row_name = ''
                            for conf_row in conf_property_list:
                                if conf_row.name != pre_conf_row_name:
                                    datatype_dict[conf_row.name] = conf_row.datatype
                                    if len(message_np) > 0 and len(pre_conf_row_name) > 0:
                                        if pre_conf_row_name in property_dict:
                                            property_dict[pre_conf_row_name] = np.concatenate([property_dict[pre_conf_row_name], message_np])
                                        else:
                                            property_dict[pre_conf_row_name] = message_np
                                        message_np = np.array([])
                                tmp_message_np = bufr_dict[conf_row.key]
                                if len(tmp_message_np) > 0:
                                    tmp_message_np = tmp_message_np[location_datetime_index_np]
                                    if len(tmp_message_np) > 0:
                                        if len(message_np) > 0:
                                            if conf_row.multiply != 0:
                                                message_np = message_np + conf_row.multiply * tmp_message_np
                                            else:
                                                message_np = message_np + tmp_message_np
                                        else:
                                            if conf_row.multiply != 0:
                                                message_np = conf_row.multiply * tmp_message_np
                                            else:
                                                message_np = tmp_message_np
                                pre_conf_row_name = conf_row.name
                            if len(message_np) > 0 and len(pre_conf_row_name) > 0:
                                if pre_conf_row_name in property_dict:
                                    property_dict[pre_conf_row_name] = np.concatenate([property_dict[pre_conf_row_name], message_np])
                                else:
                                    property_dict[pre_conf_row_name] = message_np

            if 'datetime' in location_datetime_dict:
                id_list = [id_num for id_num in range(0, len(location_datetime_dict['datetime']))]
                location_datetime_data = [pa.array(id_list, 'int32')]
                name_list = ['id']
                datetime_directory_list = []
                for key in location_datetime_dict.keys():
                    if key == 'datetime':
                        if datetime_tail == 'millisecond':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), int(dt_str[12:14]), int(dt_str[15:]), tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:11] + "0" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:11] + "0")
                        elif datetime_tail == 'second':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), int(dt_str[12:14]), 0, tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:11] + "0" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:11] + "0")
                        elif datetime_tail == 'minute':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), 0, 0, tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:11] + "0" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:11] + "0")
                        elif datetime_tail == 'hour':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), 0, 0, 0, tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:10] + "00" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:10] + "00")
                        elif datetime_tail == 'day':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), 0, 0, 0, 0, tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:8] + "0000" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:8] + "0000")
                        elif datetime_tail == 'month':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), 0, 0, 0, 0, 0, tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:6] + "000000" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:6] + "000000")
                        elif datetime_tail == 'year':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), 0, 0, 0, 0, 0, 0, tzinfo=timezone.utc) for dt_str in location_datetime_dict[key]], pa.timestamp('ms', tz='utc')))
                            for dt_str in location_datetime_dict[key]:
                                if not dt_str[0:4] + "00000000" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:4] + "00000000")
                    else:
                        location_datetime_data.append(pa.array(location_datetime_dict[key].flatten(), datatype_dict[key]))
                    name_list.append(key)
                for datetime_directory in datetime_directory_list:
                    datetime_len = 11
                    if datetime_tail == 'hour':
                        datetime_len = 10
                    elif datetime_tail == 'day':
                        datetime_len = 8
                    elif datetime_tail == 'month':
                        datetime_len = 6
                    elif datetime_tail == 'year':
                        datetime_len = 4
                    datetime_index_list = [index for index, value in enumerate(location_datetime_dict['datetime']) if value[0:datetime_len] == datetime_directory[0:datetime_len]]
                    if len(datetime_index_list) > 0:
                        tmp_location_datetime_data = [location_datetime.take(pa.array(datetime_index_list)) for location_datetime in location_datetime_data]
                        if len(tmp_location_datetime_data) > 0:
                            out_directory_list = [out_dir, cccc, 'bufr_to_arrow', cat_subcat, datetime_directory, create_datetime_directory]
                            out_directory = '/'.join(out_directory_list)
                            os.makedirs(out_directory, exist_ok=True)
                            out_file_list = [out_directory, 'location_datetime.arrow']
                            out_file = '/'.join(out_file_list)
                            with open(out_file, 'bw') as out_f:
                                location_datetime_batch = pa.record_batch(tmp_location_datetime_data, names=name_list)
                                writer = pa.ipc.new_file(out_f, location_datetime_batch.schema)
                                writer.write_batch(location_datetime_batch)
                                writer.close()
                                print(out_file, file=out_list_file)
                        for key in property_dict.keys():
                            datetime_id_list = pa.array(id_list, 'int32').take(pa.array(datetime_index_list))
                            datetime_property_data = property_dict[key][datetime_index_list]
                            value_index_list = [index for index, value in enumerate(datetime_property_data.tolist()) if value != None]
                            if len(value_index_list) > 0:
                                property_data = []
                                property_data.append(datetime_id_list.take(pa.array(value_index_list)))
                                property_data.append(datetime_property_data.take(pa.array(value_index_list)))
                                out_directory_list = [out_dir, cccc, 'bufr_to_arrow', cat_subcat, datetime_directory, create_datetime_directory]
                                out_directory = '/'.join(out_directory_list)
                                os.makedirs(out_directory, exist_ok=True)
                                out_file_list = [out_directory, key.split('[')[0].strip().replace(' ', '_') + '.arrow']
                                out_file = '/'.join(out_file_list)
                                with open(out_file, 'bw') as out_f:
                                    property_batch = pa.record_batch(property_data, names=['id', key])
                                    writer = pa.ipc.new_file(out_f, property_batch.schema)
                                    writer.write_batch(property_batch)
                                    writer.close()
                                    print(out_file, file=out_list_file)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    config = pkg_resources.resource_filename(__name__, 'conf_bufr_to_arrow.csv')
    if not re.match(r'^[A-Z]{4}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z]{4}$).', file=sys.stderr)
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
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, conf_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
