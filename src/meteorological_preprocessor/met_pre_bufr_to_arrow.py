#!/usr/bin/env python3
import argparse
import eccodes as ec
import numpy as np
import os
import pkg_resources
import pyarrow as pa
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pyarrow import csv

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
            cat = re.sub('/.*$', '', cat_subcat)
            subcat = re.sub('^.*/', '', cat_subcat)
            input_conf_df = conf_df[(conf_df['input_category'] == cat) & (conf_df['input_subcategory'] == subcat)]
            output_directory_set = set([output_tuple.output_category + '/' + output_tuple.output_subcategory for output_tuple in input_conf_df[['output_category','output_subcategory']].itertuples()])
            for output_directory in output_directory_set:
                output_directory_list = output_directory.split('/')
                output_cat = output_directory_list[0]
                output_subcat = output_directory_list[1]
                output_conf_df = input_conf_df[(input_conf_df['output_category'] == output_cat) & (input_conf_df['output_subcategory'] == output_subcat)]
                output_dict = {}
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
                    if debug:
                        print('Debug', ':', in_file, file=sys.stderr)
                    input_dict = {}
                    required_np = np.array([])
                    with open(in_file, 'rb') as in_file_stream:
                        while True:
                            bufr = None
                            try:
                                bufr = ec.codes_bufr_new_from_file(in_file_stream)
                                if bufr is None:
                                    break
                                unexpanded_descriptors = ec.codes_get_array(bufr, 'unexpandedDescriptors')
                                compressed_data = ec.codes_get(bufr, 'compressedData')
                                number_of_subsets = ec.codes_get(bufr, 'numberOfSubsets')
                                if number_of_subsets <= 0:
                                    print('Warning', warno, ':', in_file, 'numberOfSubsets is 0.', file=sys.stderr)
                                    continue
                                ec.codes_set(bufr, 'unpack', 1)
                                is_array = False
                                if len(output_conf_df[(output_conf_df['is_array'] == 1)].index) > 0:
                                    is_array = True
                                pre_value_list_len = 0
                                subset_array_size_list = []
                                for output_conf_tuple in output_conf_df.itertuples():
                                    value_list = []
                                    is_descriptors = False
                                    for descriptors in output_conf_tuple.descriptors_list.split(';'):
                                        if len(set([int(i) for i in descriptors.split(' ')]) - set(unexpanded_descriptors)) == 0:
                                            is_descriptors = True
                                            break
                                    if not is_descriptors:
                                        if pre_value_list_len == 0:
                                            print('Warning', warno, in_file, ':', output_conf_tuple, 'The first key is not in the descriptors_list.', file=sys.stderr)
                                            break
                                        else:
                                            input_dict[output_conf_tuple.key] = np.array([None for i in range(pre_value_list_len)])
                                            continue
                                    for subset_number in range(1, number_of_subsets + 1):
                                        if compressed_data == 0 and (is_array or output_conf_tuple.key_number > 0):
                                            subset_value_list = ec.codes_get_array(bufr, "/subsetNumber=" + str(subset_number) + "/" + output_conf_tuple.key)
                                            if (type(subset_value_list) is np.ndarray):
                                                subset_value_list = subset_value_list.tolist()
                                            if pre_value_list_len == 0:
                                                subset_array_size_list.append(len(subset_value_list))
                                            if output_conf_tuple.key_number > 0 and not output_conf_tuple.is_array:
                                                if len(subset_value_list) >= output_conf_tuple.key_number:
                                                    subset_value_list = [subset_value_list[output_conf_tuple.key_number - 1]]
                                                else:
                                                    subset_value_list = [None]
                                            elif output_conf_tuple.is_array:
                                                if len(subset_value_list) < subset_array_size_list[subset_number - 1]:
                                                    for none_counter in range(subset_array_size_list[subset_number - 1] - len(subset_value_list)):
                                                        subset_value_list.append(None)
                                            elif (is_array and not output_conf_tuple.is_array):
                                                if len(subset_value_list) == 1:
                                                    subset_value_list = [subset_value_list[0] for i in range(subset_array_size_list[subset_number - 1])]
                                            value_list = value_list + subset_value_list
                                        else:
                                            if compressed_data != 0:
                                                if output_conf_tuple.key_number > 0:
                                                    value_list = ec.codes_get_array(bufr, '#' + str(output_conf_tuple.key_number) + '#' + output_conf_tuple.key)
                                                else:
                                                    value_list = ec.codes_get_array(bufr, output_conf_tuple.key)
                                                if len(value_list) == 1:
                                                    value_list = [value_list[0] for i in range(pre_value_list_len)]
                                            else:
                                                value_list = ec.codes_get_array(bufr, output_conf_tuple.key)
                                            if (type(value_list) is np.ndarray):
                                                value_list = value_list.tolist()
                                            break
                                    if pre_value_list_len > 0:
                                        if pre_value_list_len != len(value_list):
                                            print('Warning', warno, in_file, ':', output_conf_tuple.key, ' is not equals to pre_value_list_len.', file=sys.stderr)
                                            break
                                    pre_value_list_len = len(value_list)
                                    value_np = np.array(value_list)
                                    if np.issubdtype(value_np.dtype, np.integer):
                                        value_np = np.where(value_np == ec.CODES_MISSING_LONG, None, value_np)
                                    elif np.issubdtype(value_np.dtype, float):
                                        value_np = np.where(value_np == ec.CODES_MISSING_DOUBLE, None, value_np)
                                    elif np.issubdtype(value_np.dtype, str):
                                        value_np = np.array([string.strip() for string in value_np], dtype=str)
                                    elif np.issubdtype(value_np.dtype, object):
                                        if isinstance(value_np[0], int):
                                            value_np = np.where(value_np == ec.CODES_MISSING_LONG, None, value_np)
                                        elif isinstance(value_np[0], float):
                                            value_np = np.where(value_np == ec.CODES_MISSING_DOUBLE, None, value_np)
                                        elif isinstance(value_np[0], str):
                                            value_np = np.array([string.strip() for string in value_np], dtype=str)
                                    if output_conf_tuple.name == 'longitude [degree]':
                                        value_np = np.where(value_np == 180, -180, value_np)
                                    if output_conf_tuple.slide > -1 and output_conf_tuple.step > 0:
                                        value_np = value_np[output_conf_tuple.slide::output_conf_tuple.step]
                                    if output_conf_tuple.is_required:
                                        tmp_required_np = np.array([False if value == None else True for value in value_np])
                                        if len(required_np) > 0:
                                            required_np = required_np * tmp_required_np
                                        else:
                                            required_np = tmp_required_np
                                    if output_conf_tuple.key_number > 0:
                                        input_dict[output_conf_tuple.key + '#' + str(output_conf_tuple.key_number)] = value_np
                                    else:
                                        input_dict[output_conf_tuple.key] = value_np
                                if len(required_np) <= 0 or True not in required_np:
                                    continue
                                index_np = np.array([index for index, value in enumerate(required_np) if value == True])
                                for input_dict_key in input_dict.keys():
                                    input_dict[input_dict_key] = input_dict[input_dict_key][index_np]                                
                                datetime_np = np.array([])
                                year_np = input_dict['year']
                                month_np = input_dict['month']
                                day_np = input_dict['day']
                                time_dict = {}
                                is_time_type_end = False
                                for time_type in ['millisecond', 'second', 'minute', 'hour']:
                                    if time_type not in input_dict:
                                        time_dict[time_type] = np.array([0 for value in year_np])
                                    elif not is_time_type_end:
                                        time_dict[time_type] = np.array([0 if value == None else value for value in input_dict[time_type]])
                                        is_time_type_end = True
                                    else:
                                        time_dict[time_type] = input_dict[time_type]
                                hour_np = time_dict['hour']
                                minute_np = time_dict['minute']
                                second_np = time_dict['second']
                                millisecond_np = time_dict['millisecond']
                                timedelta_millisecond_list = [0 for value in year_np]
                                timedelta_second_list = [0 for value in year_np]
                                for timedelta_conf_tuple in output_conf_df[(output_conf_df['name'] == 'timedelta [millisecond]') | (output_conf_df['name'] == 'timedelta [second]')].itertuples():
                                    if timedelta_conf_tuple.name == 'timedelta [second]':
                                        timedelta_second_list = [0 if value == None else value for value in input_dict[timedelta_conf_tuple.key]]
                                    elif timedelta_conf_tuple.name == 'timedelta [millisecond]':
                                        timedelta_millisecond_list = [0 if value == None else value for value in input_dict[timedelta_conf_tuple.key]]
                                datetime_list = []
                                for i, year in enumerate(year_np):
                                    datetime_list.append(datetime(year, month_np[i], day_np[i], hour_np[i], minute_np[i], second_np[i], millisecond_np[i], tzinfo=timezone.utc) + timedelta(seconds=timedelta_second_list[i]) + timedelta(milliseconds=timedelta_millisecond_list[i]))
                                input_dict['datetime'] = np.array(datetime_list)
                                calc_dict = {}
                                pre_name = ''
                                for output_conf_tuple in output_conf_df.itertuples():
                                    if output_conf_tuple.key == 'year':
                                        input_np = input_dict['datetime']
                                    elif output_conf_tuple.key in ['month', 'day', 'hour', 'minute', 'second', 'millisecond'] or output_conf_tuple.name in ['timedelta [second]', 'timedelta [millisecond]']:
                                        continue
                                    else:
                                        if output_conf_tuple.key_number > 0 and output_conf_tuple.key + '#' + str(output_conf_tuple.key_number) in input_dict:
                                            input_np = input_dict[output_conf_tuple.key + '#' + str(output_conf_tuple.key_number)]
                                        elif output_conf_tuple.key in input_dict:
                                            input_np = input_dict[output_conf_tuple.key]
                                        else:
                                            continue
                                        if output_conf_tuple.key == 'latitudeDisplacement' or output_conf_tuple.key == 'longitudeDisplacement':
                                            input_np = np.array([0.0 if value == None else value for value in input_np])
                                    if output_conf_tuple.name in calc_dict:
                                        tmp_np = calc_dict[output_conf_tuple.name]
                                        if output_conf_tuple.multiply != 0:
                                            calc_dict[output_conf_tuple.name] = tmp_np + output_conf_tuple.multiply * input_np
                                        else:
                                            calc_dict[output_conf_tuple.name] = tmp_np + input_np
                                    else:
                                        if output_conf_tuple.multiply != 0:
                                            calc_dict[output_conf_tuple.name] = output_conf_tuple.multiply * input_np
                                        else:
                                            calc_dict[output_conf_tuple.name] = input_np
                                    if len(pre_name) > 0 and pre_name != output_conf_tuple.name:
                                        if pre_name in output_dict:
                                            output_dict[pre_name] = output_dict[pre_name] + calc_dict[pre_name].tolist()
                                        else:
                                            output_dict[pre_name] = calc_dict[pre_name].tolist()
                                    pre_name = output_conf_tuple.name
                                if len(pre_name) > 0:
                                    if pre_name in output_dict:
                                        output_dict[pre_name] = output_dict[pre_name] + calc_dict[pre_name].tolist()
                                    else:
                                        output_dict[pre_name] = calc_dict[pre_name].tolist()
                                ec.codes_release(bufr)
                            except:
                                traceback.print_exc(file=sys.stderr)
                                break
                if 'datetime' in output_dict:
                    output_conf_name_np = np.array(list(set([output_conf_tuple.name for output_conf_tuple in output_conf_df.itertuples()])), dtype=str)
                    name_list = []
                    field_list = []
                    data_list = []
                    is_required_list = []
                    for output_conf_name in np.sort(output_conf_name_np):
                        if '@' in output_conf_name:
                            if output_conf_name.split('@')[0] not in name_list:
                                continue
                        if output_conf_name in output_dict and any([False if value == None else True for value in output_dict[output_conf_name]]):
                            for output_conf_name_tuple in output_conf_df[(output_conf_df['name'] == output_conf_name)].head(1).itertuples():
                                if debug:
                                    print('Debug', ':', 'output_conf_name =', output_conf_name, file=sys.stderr)
                                if output_conf_name_tuple.data_type == 'timestamp':
                                    field_list.append(pa.field(output_conf_name, pa.timestamp('ms', tz='utc'), nullable=not output_conf_name_tuple.is_required))
                                else:
                                    field_list.append(pa.field(output_conf_name, output_conf_name_tuple.data_type, nullable=not output_conf_name_tuple.is_required))
                                data_list.append(pa.array(output_dict[output_conf_name]))
                                name_list.append(output_conf_name)
                                is_required_list.append(output_conf_name_tuple.is_required)
                    if len(field_list) <=0 or False not in is_required_list:
                        continue
                    out_directory_list = [out_dir, cccc, 'bufr_to_arrow', output_cat, output_subcat]
                    out_directory = '/'.join(out_directory_list)
                    os.makedirs(out_directory, exist_ok=True)
                    now = datetime.utcnow()
                    out_file_list = [out_directory, '/', 'C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2), '.arrow']
                    out_file = ''.join(out_file_list)
                    batch = pa.record_batch(data_list, pa.schema(field_list))
                    with open(out_file, 'bw') as out_f:
                        ipc_writer = pa.ipc.new_file(out_f, batch.schema, options=pa.ipc.IpcWriteOptions(compression=pa.Codec('zstd', compression_level=16)))
                        ipc_writer.write_batch(batch)
                        ipc_writer.close()
                        print(out_file, file=out_list_file)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument("--config", type=str, metavar='conf_bufr_to_arrow.csv', default=pkg_resources.resource_filename(__name__, 'conf_bufr_to_arrow.csv'))
    parser.add_argument("--debug", action='store_true')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    args = parser.parse_args()
    if not re.match(r'^[A-Z][A-Z0-9]{3}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z][A-Z0-9]{3}$).', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(args.config, os.F_OK):
        print('Error', errno, ':', args.config, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config):
        print('Error', errno, ':', args.config, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config, os.R_OK):
        print('Error', errno, ':', args.config, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        conf_df = csv.read_csv(args.config).to_pandas()
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, conf_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
