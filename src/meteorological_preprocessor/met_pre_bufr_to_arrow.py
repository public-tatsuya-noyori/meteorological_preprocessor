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

def getArray(bufr, subset_num, subset_len, conf_row, in_file):
    array = np.array([None])
    is_success = True
    try:
        if subset_num > 0:
            array = codes_get_array(bufr, "/subsetNumber=" + str(subset_num) + "/" + conf_row.key)
        else:
            array = codes_get_array(bufr, conf_row.key)
    except:
        is_success = False
    if not is_success and subset_num > 0:
        try:
            array = codes_get_array(bufr, conf_row.key)
            if len(array) == subset_len:
                value = array[subset_num - 1]
                array = np.array([value])
                is_success = True
            elif len(array) == 1:
                value = array[0]
                array = np.array([value])
                is_success = True
            else:
                is_success = False
        except:
            is_success = False
    if is_success:
        if conf_row.datatype == 'string':
            array = np.array([value.lstrip().rstrip() for value in array], dtype=object)
        else:
            array = np.where(np.isnan(array), None, array)
            if conf_row.name == 'datetime':
                if conf_row.key == 'year':
                    array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(4) for value in array], dtype=object)
                elif conf_row.key == 'month':
                    array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in array], dtype=object)
                elif conf_row.key == 'day':
                    array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in array], dtype=object)
                elif conf_row.key == 'hour':
                    array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in array], dtype=object)
                elif conf_row.key == 'minute':
                    array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in array], dtype=object)
                elif conf_row.key == 'second':
                    if not np.isnan(conf_row.missing):
                        array = np.array([str(int(conf_row.missing)).zfill(2) if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in array], dtype=object)
                    else:
                        array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(2) for value in array], dtype=object)
                elif conf_row.key == 'millisecond':
                    if not np.isnan(conf_row.missing):
                        array = np.array([str(int(conf_row.missing)).zfill(3) if value < conf_row.min or value > conf_row.max else str(value).zfill(3) for value in array], dtype=object)
                    else:
                        array = np.array([None if value < conf_row.min or value > conf_row.max else str(value).zfill(3) for value in array], dtype=object)
            elif not np.isnan(conf_row.missing):
                array = np.array([conf_row.missing if value < conf_row.min or value > conf_row.max else value for value in array], dtype=object)
            else:
                array = np.array([None if value < conf_row.min or value > conf_row.max else value for value in array], dtype=object)
            if conf_row.name == 'longitude [degree]':
                array = np.where(array == conf_row.min, conf_row.max, array)
            if conf_row.slide > -1 and conf_row.step > 0:
                array = array[conf_row.slide::conf_row.step]
    else:
        if conf_row.output == 'location_datetime':
            print('Info', ': sub ', 'can not get array.', conf_row.key, in_file, file=sys.stderr)
        array = np.array([None])
    return array

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
            descriptor_conf_df = None
            datatype_dict = {}
            property_dict = {}
            output_property_dict = {}
            for in_file in in_file_list:
                match = re.search(r'^.*/' + cccc + '/bufr/' + cat_subcat + '/.*$', in_file)
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
                        codes_set(bufr, 'unpack', 1)
                        unexpanded_descriptors = codes_get_array(bufr, 'unexpandedDescriptors')                    
                        descriptor_conf_df = pd.DataFrame(index=[], columns=['descriptor','descriptor_2'])
                        for bufr_descriptor in unexpanded_descriptors:
                            cat = re.sub('/.*$', '', cat_subcat)
                            subcat = re.sub('^.*/', '', cat_subcat)
                            descriptor_conf_df = conf_df[(conf_df['category'] == cat) & (conf_df['subcategory'] == subcat) & (conf_df['descriptor'] == bufr_descriptor)]
                            if len(descriptor_conf_df) > 0:
                                descriptor_2_list = list(set(descriptor_conf_df[['descriptor_2']].values.flatten()))
                                if len(descriptor_2_list) > 0 and not np.isnan(descriptor_2_list[0]):
                                    is_descriptor_2 = False
                                    for descriptor_2 in descriptor_2_list:
                                        if descriptor_2 in unexpanded_descriptors:
                                            descriptor_conf_df = descriptor_conf_df[descriptor_conf_df['descriptor_2'] == descriptor_2]
                                            is_descriptor_2 = True
                                            break
                                    if not is_descriptor_2:
                                        descriptor_conf_df = pd.DataFrame(index=[], columns=['descriptor','descriptor_2'])
                                break
                        if len(descriptor_conf_df) == 0:
                            print('Info', ':', 'not found descriptor.', unexpanded_descriptors, in_file, file=sys.stderr)
                            break
                        number_of_subsets = codes_get(bufr, 'numberOfSubsets')
                        if number_of_subsets == 0:
                            break
                        bufr_dict = {}
                        none_np = np.array([])
                        number_of_array = 0
                        for conf_row in descriptor_conf_df.itertuples():
                            if conf_row.get_type == 'subset':
                                for subset_num in range(1, number_of_subsets + 1):
                                    array = getArray(bufr, subset_num, number_of_subsets, conf_row, in_file)
                                    if conf_row.convert_type == 'to_value' or conf_row.convert_type == 'to_value_to_array':
                                        if len(array) > conf_row.array_index:
                                            value = array[int(conf_row.array_index)]
                                            if conf_row.convert_type == 'to_value_to_array':
                                                array = np.array([value for i in range(0, number_of_array)], dtype=object)
                                            else:
                                                array = np.array([value], dtype=object)
                                        else:
                                            if conf_row.output == 'location_datetime':
                                                print('Info', ':', 'conf_row.array_index is more than len(array).', conf_row.key, in_file, file=sys.stderr)
                                                break
                                            else:
                                                continue
                                    #else:    
                                        #if len(array) < number_of_array:
                                        #    array = np.concatenate([array, np.array([None for i in range(0, number_of_array - len(array))])])
                                        #else:
                                        #    array = array[0:number_of_array]
                                    if number_of_array == 0:
                                        number_of_array = len(array)

                                    if len(array) != number_of_array:
                                        print('Warning', warno, ': subset :', conf_row.key, len(array), number_of_array, 'The length of array is not equals to the number of array.', in_file, file=sys.stderr)
                                        if conf_row.output == 'location_datetime':
                                            bufr_dict = {}
                                            break
                                        else:
                                            continue
                                    if conf_row.key in bufr_dict:
                                        bufr_dict[conf_row.key] = np.concatenate([bufr_dict[conf_row.key], array])
                                    else:
                                        bufr_dict[conf_row.key] = array
                                if conf_row.key in bufr_dict:
                                    array = bufr_dict[conf_row.key]
                                else:
                                    if conf_row.output == 'location_datetime':
                                        bufr_dict = {}
                                        break
                                    else:
                                        continue
                            else:
                                array = getArray(bufr, 0, 0, conf_row, in_file)
                                if number_of_array == 0:
                                    number_of_array = len(array)
                                if len(array) == 1:
                                    value = array[0]
                                    array = np.array([value for i in range(0, number_of_array)], dtype=object)
                                if len(array) != number_of_array:
                                    if conf_row.output == 'location_datetime':
                                        print('Warning', warno, ':', conf_row.key, len(array), number_of_array, 'The length of array is not equals to the number of array.', in_file, file=sys.stderr)
                                        bufr_dict = {}
                                        break
                                    else:
                                        print('Warning', warno, ':', conf_row.key, len(array), number_of_array, 'The length of array is not equals to the number of array.', in_file, file=sys.stderr)
                                        continue
                            bufr_dict[conf_row.key] = array
                            if conf_row.output == 'location_datetime':
                                tmp_none_np = np.array([False if value == None else True for value in array])
                                if len(none_np) > 0:
                                    none_np = none_np * tmp_none_np
                                else:
                                    none_np = tmp_none_np
                            bufr_dict['none'] = none_np
                        codes_release(bufr)
                        if len(bufr_dict) == 0:
                            break
                        location_datetime_index_np = np.array([index for index, value in enumerate(bufr_dict['none']) if value == True])
                        if len(location_datetime_index_np) > 0:
                            message_np = np.array([])
                            pre_conf_row_name = ''
                            for conf_row in descriptor_conf_df.itertuples():
                                if conf_row.name != pre_conf_row_name:
                                    datatype_dict[conf_row.name] = conf_row.datatype
                                    if conf_row.output != 'location_datetime':
                                        if conf_row.output in output_property_dict:
                                            tmp_output_property_list = output_property_dict[conf_row.output]
                                            if not conf_row.name in tmp_output_property_list:
                                                tmp_output_property_list.append(conf_row.name)
                                                output_property_dict[conf_row.output] = tmp_output_property_list
                                        else:
                                            output_property_dict[conf_row.output] = [conf_row.name]
                                    if len(message_np) > 0 and len(pre_conf_row_name) > 0:
                                        if pre_conf_row_name in property_dict:
                                            property_dict[pre_conf_row_name] = np.concatenate([property_dict[pre_conf_row_name], message_np])
                                        else:
                                            property_dict[pre_conf_row_name] = message_np
                                        message_np = np.array([])
                                if conf_row.key in bufr_dict:
                                    tmp_message_np = bufr_dict[conf_row.key]
                                    if max(location_datetime_index_np) < len(tmp_message_np):
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
                                    else:
                                        print('Info', 'unexpanded_descriptors :', unexpanded_descriptors, ': conditon of', conf_row.key, max(location_datetime_index_np), len(tmp_message_np), in_file, file=sys.stderr)
                                pre_conf_row_name = conf_row.name
                            if len(message_np) > 0 and len(pre_conf_row_name) > 0:
                                if pre_conf_row_name in property_dict:
                                    property_dict[pre_conf_row_name] = np.concatenate([property_dict[pre_conf_row_name], message_np])
                                else:
                                    property_dict[pre_conf_row_name] = message_np
            if 'datetime' in property_dict:
                id_list = [id_num for id_num in range(0, len(property_dict['datetime']))]
                location_datetime_data = [pa.array(id_list, 'int32')]
                location_datetime_name_list = ['id']
                datetime_directory_list = []
                del_key_list = []
                datetime_tail = descriptor_conf_df[(descriptor_conf_df['name'] == 'datetime')]['key'].values.flatten()[-1]
                for conf_row_name in set(descriptor_conf_df[(descriptor_conf_df['output'] == 'location_datetime')]['name'].values.flatten()):
                    if conf_row_name == 'datetime':
                        plus_second_list = [0 for dt in range(0, len(property_dict[conf_row_name]))]
                        if 'time period [s]' in property_dict:
                            plus_second_list = property_dict['time period [s]']
                            del_key_list.append('time period [s]')
                        if datetime_tail == 'millisecond':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), int(dt_str[12:14]), int(dt_str[15:]), tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:11] + "0" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:11] + "0")
                        elif datetime_tail == 'second':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), int(dt_str[12:14]), 0, tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:11] + "0" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:11] + "0")
                        elif datetime_tail == 'minute':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), 0, 0, tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:11] + "0" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:11] + "0")
                        elif datetime_tail == 'hour':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:10] + "00" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:10] + "00")
                        elif datetime_tail == 'day':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), 0, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:8] + "0000" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:8] + "0000")
                        elif datetime_tail == 'month':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), int(dt_str[4:6]), 0, 0, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:6] + "000000" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:6] + "000000")
                        elif datetime_tail == 'year':
                            location_datetime_data.append(pa.array([datetime(int(dt_str[0:4]), 0, 0, 0, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=plus_second_list[i]) for i, dt_str in enumerate(property_dict[conf_row_name])], pa.timestamp('ms', tz='utc')))
                            for dt_str in property_dict[conf_row_name]:
                                if not dt_str[0:4] + "00000000" in datetime_directory_list:
                                    datetime_directory_list.append(dt_str[0:4] + "00000000")
                        location_datetime_name_list.append(conf_row_name)
                    elif conf_row_name != 'time period [s]':
                        location_datetime_data.append(pa.array(property_dict[conf_row_name].flatten(), datatype_dict[conf_row_name]))
                        location_datetime_name_list.append(conf_row_name)
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
                    datetime_index_list = [index for index, value in enumerate(property_dict['datetime']) if value[0:datetime_len] == datetime_directory[0:datetime_len]]
                    if len(datetime_index_list) > 0:
                        tmp_location_datetime_data = [location_datetime.take(pa.array(datetime_index_list)) for location_datetime in location_datetime_data]
                        if len(tmp_location_datetime_data) > 0:
                            out_directory_list = [out_dir, cccc, 'bufr_to_arrow', cat_subcat, datetime_directory, create_datetime_directory]
                            out_directory = '/'.join(out_directory_list)
                            os.makedirs(out_directory, exist_ok=True)
                            out_file_list = [out_directory, 'location_datetime.arrow']
                            out_file = '/'.join(out_file_list)
                            with open(out_file, 'bw') as out_f:
                                location_datetime_batch = pa.record_batch(tmp_location_datetime_data, names=location_datetime_name_list)
                                writer = pa.ipc.new_file(out_f, location_datetime_batch.schema)
                                writer.write_batch(location_datetime_batch)
                                writer.close()
                                print(out_file, file=out_list_file)
                            for output in output_property_dict.keys():
                                property_name_list = ['id']
                                property_data = []
                                datetime_id_pa = pa.array(id_list, 'int32').take(pa.array(datetime_index_list))
                                value_index_list = []
                                if output_property_dict[output] != None:
                                    datetime_property_data_dict = {}
                                    for property_key in output_property_dict[output]:
                                        property_name_list.append(property_key)
                                        if property_key in property_dict:
                                            if max(datetime_index_list) < len(property_dict[property_key]):
                                                datetime_property_data = pa.array(property_dict[property_key][datetime_index_list].tolist(), datatype_dict[property_key])
                                                datetime_property_data_dict[property_key] = datetime_property_data
                                                if len(value_index_list) > 0:
                                                    value_index_list = list(set(value_index_list) & set([index for index, value in enumerate(datetime_property_data.tolist()) if value != None]))
                                                else:
                                                    value_index_list = [index for index, value in enumerate(datetime_property_data.tolist()) if value != None]
                                            else:
                                                print('Info', cat_subcat, 'max(datetime_index_list) >= len(property_dict[property_key]) key :', property_key, max(datetime_index_list), len(property_dict[property_key]), file=sys.stderr)
                                    if len(value_index_list) > 0:
                                        property_data.append(datetime_id_pa.take(pa.array(value_index_list)))
                                        is_output = True
                                        for property_key in output_property_dict[output]:
                                            if property_key in datetime_property_data_dict:
                                                property_data.append(datetime_property_data_dict[property_key].take(pa.array(value_index_list)))
                                            else:
                                                print('Info', cat_subcat, 'key :', property_key, 'no data', file=sys.stderr)
                                                is_output = False
                                        if is_output:
                                            out_directory_list = [out_dir, cccc, 'bufr_to_arrow', cat_subcat, datetime_directory, create_datetime_directory]
                                            out_directory = '/'.join(out_directory_list)
                                            os.makedirs(out_directory, exist_ok=True)
                                            out_file_list = [out_directory, output + '.arrow']
                                            out_file = '/'.join(out_file_list)
                                            with open(out_file, 'bw') as out_f:
                                                property_batch = pa.record_batch(property_data, names=property_name_list)
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
