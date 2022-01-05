#!/usr/bin/env python3
import argparse
import eccodes as ec
import gribapi
import math
import numpy as np
import os
import pkg_resources
import pyarrow as pa
import pandas as pd
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pyarrow import csv

def convert_to_dataset(cccc, cat, subcat, in_df, out_dir, out_list_file, conf_df, debug):
    warno = 188
    created_second = int(math.floor(datetime.utcnow().timestamp()))
    for conf_tuple in conf_df[(conf_df['category'] == cat) & (conf_df['subcategory'] == subcat)].itertuples():
        sort_unique_list = conf_tuple.sort_unique_list.split(';')
        tile_level = conf_tuple.tile_level
        out_file_dict = {}
        new_datetime_list_dict = {}
        res = 180 / 2**tile_level
        for tile_x in range(0, 2**(tile_level + 1)):
            for tile_y in range(0, 2**(tile_level)):
                if tile_y == 2**(tile_level) - 1:
                    tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]'])]
                else:
                    tile_df = in_df[(res * tile_x - 180.0 <= in_df['longitude [degree]']) & (in_df['longitude [degree]'] < res * (tile_x + 1) - 180.0) & (90.0 - res * tile_y >= in_df['latitude [degree]']) & (in_df['latitude [degree]'] > 90.0 - res * (tile_y + 1))]
                new_datetime_list_dict[tile_x,  tile_y] = tile_df['datetime'].dt.floor(str(conf_tuple.minute_level) + 'T').unique()
                for new_datetime in new_datetime_list_dict[tile_x,  tile_y]:
                    new_df = tile_df[(new_datetime - (timedelta(minutes=conf_tuple.minute_level) / 2) <= tile_df['datetime']) & (tile_df['datetime'] < new_datetime + (timedelta(minutes=conf_tuple.minute_level) / 2))]
                    out_file = ''.join([out_dir, '/', cccc, '/bufr_to_arrow/', cat, '/', subcat, '/', str(new_datetime.year).zfill(4), '/', str(new_datetime.month).zfill(2), str(new_datetime.day).zfill(2), '/', str(new_datetime.hour).zfill(2), str(new_datetime.minute).zfill(2), '/l', str(tile_level), 'x', str(tile_x), 'y', str(tile_y), '.arrow'])
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
                        if out_file in out_file_dict:
                            former_df = out_file_dict[out_file]
                        elif os.path.exists(out_file):
                            former_ipc_reader = pa.ipc.open_file(out_file)
                            former_df = former_ipc_reader.read_pandas()
                        else:
                            former_df = pd.DataFrame()
                        if len(former_df.index) > 0:
                            new_df = pd.concat([former_df, new_df])
                            tmp_sort_unique_list = list(set(new_df.columns) & set(sort_unique_list))
                            tmp_sort_unique_list.insert(0, 'indicator')
                            tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                            new_df.sort_values(tmp_sort_unique_list, inplace=True)
                            tmp_sort_unique_list.remove('created time minus data time [s]')
                            new_df.drop_duplicates(subset=tmp_sort_unique_list, keep='last', inplace=True)
                            tmp_sort_unique_list.insert(1, 'created time minus data time [s]')
                        out_file_dict[out_file] = new_df
        for out_file, out_df in out_file_dict.items():
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            table = pa.Table.from_pandas(out_df.reset_index(drop=True)).replace_schema_metadata(metadata=None)
            with open(out_file, 'bw') as out_f:
                #ipc_writer = pa.ipc.new_file(out_f, table.schema, options=pa.ipc.IpcWriteOptions(compression='zstd'))
                ipc_writer = pa.ipc.new_file(out_f, table.schema, options=pa.ipc.IpcWriteOptions(compression=None))
                for batch in table.to_batches():
                    ipc_writer.write_batch(batch)
                ipc_writer.close()
                print(out_file, file=out_list_file)

def convert_to_arrow(in_file_list, conf_df, out_dir, out_list_file, conf_bufr_arrow_to_dataset_df, debug):
    warno = 189
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
                output_data_type_dict = {}
                output_is_required_dict = {}
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
                    name_with_att_dict = {}
                    out_name_list = []
                    is_array = False
                    is_first_key = True
                    first_key = ''
                    first_key_value_np_len = 0
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
                                ec.codes_set(bufr, 'unpack', 1)
                                if len(output_conf_df[(output_conf_df['key_number'] == 0)].index) > 0:
                                    is_array = True
                                subset_array_size_list = []
                                for output_conf_tuple in output_conf_df.itertuples():
                                    value_list = []
                                    is_descriptors = False
                                    for descriptors in output_conf_tuple.descriptors_list.split(';'):
                                        if len(set([int(i) for i in descriptors.split(' ')]) - set(unexpanded_descriptors)) == 0:
                                            is_descriptors = True
                                            break
                                    if is_descriptors:
                                        if compressed_data == 0:
                                            for subset_number in range(1, number_of_subsets + 1):
                                                try:
                                                    subset_value_list = ec.codes_get_array(bufr, "/subsetNumber=" + str(subset_number) + "/" + output_conf_tuple.key)
                                                    if (type(subset_value_list) is np.ndarray):
                                                        subset_value_list = subset_value_list.tolist()
                                                    if is_array:
                                                        if is_first_key:
                                                            subset_array_size_list.append(len(subset_value_list))
                                                            value_list.extend(subset_value_list)
                                                        else:
                                                            if len(subset_value_list) == 1:
                                                                for i in range(subset_array_size_list[subset_number - 1]):
                                                                    value_list.append(subset_value_list[0])
                                                            elif len(subset_value_list) <= subset_array_size_list[subset_number - 1]:
                                                                value_list.extend(subset_value_list)
                                                                for none_counter in range(subset_array_size_list[subset_number - 1] - len(subset_value_list)):
                                                                    value_list.append(None)
                                                            elif len(subset_value_list) < subset_array_size_list[subset_number - 1] * 2:
                                                                value_list.extend(subset_value_list[0:subset_array_size_list[subset_number - 1] - len(subset_value_list)])
                                                            else:
                                                                value_list.extend(subset_value_list)
                                                            
                                                    else:
                                                        if output_conf_tuple.key_number <= len(subset_value_list):
                                                            value_list.append(subset_value_list[output_conf_tuple.key_number - 1])
                                                        else:
                                                            value_list.append(None)
                                                except gribapi.errors.KeyValueNotFoundError:
                                                    if is_array:
                                                        if is_first_key:
                                                            subset_array_size_list.append(0)
                                                        else:
                                                            for i in range(subset_array_size_list[subset_number - 1]):
                                                                value_list.append(None)
                                                    else:
                                                        value_list.append(None)
                                        else:
                                            if output_conf_tuple.key_number > 0:
                                                try:
                                                    value_list = ec.codes_get_array(bufr, '#' + str(output_conf_tuple.key_number) + '#' + output_conf_tuple.key)
                                                except gribapi.errors.KeyValueNotFoundError:
                                                    if is_first_key:
                                                        value_list = [None for i in range(number_of_subsets)]
                                                    else:
                                                        value_list = [None for i in range(first_key_value_np_len)]
                                            else:
                                                try:
                                                    value_list = ec.codes_get_array(bufr, output_conf_tuple.key)
                                                except gribapi.errors.KeyValueNotFoundError:
                                                    if is_first_key:
                                                        value_list = [None for i in range(number_of_subsets)]
                                                    else:
                                                        value_list = [None for i in range(first_key_value_np_len)]
                                            if (type(value_list) is np.ndarray):
                                                value_list = value_list.tolist()
                                            if len(value_list) == 1:
                                                if is_first_key:
                                                    value_list = [value_list[0] for i in range(number_of_subsets)]
                                                else:
                                                    value_list = [value_list[0] for i in range(first_key_value_np_len)]
                                        if is_first_key:
                                            if len(value_list) == 0:
                                               break
                                        value_np = np.array(value_list)
                                        if np.issubdtype(value_np.dtype, np.integer):
                                            value_np = np.where(value_np == ec.CODES_MISSING_LONG, None, value_np)
                                        elif np.issubdtype(value_np.dtype, float):
                                            value_np = np.where(value_np == ec.CODES_MISSING_DOUBLE, None, value_np)
                                        elif np.issubdtype(value_np.dtype, str):
                                            value_np = np.array([string.strip() for string in value_np], dtype=object)
                                        elif np.issubdtype(value_np.dtype, object):
                                            value_list = []
                                            for value in value_np:
                                                if isinstance(value, int) and value == ec.CODES_MISSING_LONG:
                                                    value_list.append(None)
                                                elif isinstance(value, float) and value == ec.CODES_MISSING_DOUBLE:
                                                    value_list.append(None)
                                                elif isinstance(value, str):
                                                    value_list.append(value.strip())
                                                else:
                                                    value_list.append(value)
                                            value_np = np.array(value_list)
                                        if output_conf_tuple.name == 'longitude [degree]':
                                            value_np = np.where(value_np == 180, -180, value_np)
                                        if output_conf_tuple.slide > -1 and output_conf_tuple.step > 0:
                                            value_np = value_np[output_conf_tuple.slide::output_conf_tuple.step]
                                        if output_conf_tuple.is_abs:
                                            value_np = np.array([None if value == None else abs(value) for value in value_np])
                                        if output_conf_tuple.is_str:
                                            value_np = np.array([None if value == None else str(value) for value in value_np], dtype=object)
                                        if output_conf_tuple.zfill > 0:
                                            value_np = np.array([None if value == None else value.zfill(output_conf_tuple.zfill) for value in value_np], dtype=object)
                                        if len(output_conf_tuple.plus_str) > 0:
                                            value_np = np.array([None if value == None else value + output_conf_tuple.plus_str for value in value_np], dtype=object)
                                        if is_first_key:
                                            if np.all(value_np == None):
                                                break
                                            is_first_key = False
                                            first_key = output_conf_tuple.name
                                            first_key_value_np_len = len(value_np)
                                        else:
                                            if first_key_value_np_len != len(value_np):
                                                print('Warning', warno, in_file, ':', first_key_value_np_len, ':', len(value_np), output_conf_tuple.key, 'is not equals to first_key_value_np_len.', file=sys.stderr)
                                                break
                                        if output_conf_tuple.is_required:
                                            tmp_required_np = np.array([False if value == None else True for value in value_np])
                                            if len(required_np) > 0:
                                                required_np = required_np * tmp_required_np
                                            else:
                                                required_np = tmp_required_np
                                        if output_conf_tuple.key in ['year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond']:
                                            input_dict[output_conf_tuple.key] = value_np
                                        else:
                                            input_dict[output_conf_tuple.key + '#' + str(output_conf_tuple.key_number) + '#' + output_conf_tuple.name] = value_np
                                    else:
                                        if is_first_key:
                                            if debug:
                                                print('Warning', warno, in_file, ':', output_conf_tuple, 'The first key is not in the descriptors_list.', file=sys.stderr)
                                            break
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
                                if np.issubdtype(second_np.dtype, float):
                                    millisecond_np = np.array([None if value == None else math.floor(value * 1000) - (math.floor(value) * 1000) for value in second_np])
                                    second_np = np.array([None if value == None else math.floor(value) for value in second_np])
                                else:
                                    millisecond_np = time_dict['millisecond']
                                timedelta_millisecond_list = [0 for value in year_np]
                                timedelta_second_list = [0 for value in year_np]
                                for timedelta_conf_tuple in output_conf_df[(output_conf_df['name'] == 'timedelta [millisecond]') | (output_conf_df['name'] == 'timedelta [second]')].itertuples():
                                    if timedelta_conf_tuple.name == 'timedelta [second]':
                                        timedelta_second_list = [0 if value == None else value for value in input_dict[timedelta_conf_tuple.key + '#' + str(timedelta_conf_tuple.key_number) + '#' + timedelta_conf_tuple.name]]
                                    elif timedelta_conf_tuple.name == 'timedelta [millisecond]':
                                        timedelta_millisecond_list = [0 if value == None else value for value in input_dict[timedelta_conf_tuple.key + '#' + str(timedelta_conf_tuple.key_number) + '#' + timedelta_conf_tuple.name]]
                                datetime_list = []
                                for i, year in enumerate(year_np):
                                    datetime_list.append(datetime(year, month_np[i], day_np[i], hour_np[i], minute_np[i], second_np[i], millisecond_np[i] * 1000, tzinfo=timezone.utc) + timedelta(seconds=timedelta_second_list[i]) + timedelta(milliseconds=timedelta_millisecond_list[i]))
                                input_dict['datetime'] = np.array(datetime_list)
                                plus_dict = {}
                                pre_name = ''
                                pre_data_type = ''
                                for output_conf_tuple in output_conf_df.itertuples():
                                    if output_conf_tuple.key == 'year':
                                        input_np = input_dict['datetime']
                                    elif output_conf_tuple.name in ['datetime', 'timedelta [second]', 'timedelta [millisecond]']:
                                        continue
                                    else:
                                        if output_conf_tuple.key + '#' + str(output_conf_tuple.key_number) + '#' + output_conf_tuple.name in input_dict:
                                            input_np = input_dict[output_conf_tuple.key + '#' + str(output_conf_tuple.key_number) + '#' + output_conf_tuple.name]
                                        else:
                                            continue
                                        if output_conf_tuple.key == 'latitudeDisplacement' or output_conf_tuple.key == 'longitudeDisplacement':
                                            input_np = np.array([0.0 if value == None else value for value in input_np])
                                    output_conf_tuple_name = output_conf_tuple.name
                                    if len(output_conf_tuple_name) > 0:
                                        if '{' in output_conf_tuple_name:
                                            if len(pre_name) > 0 and '@' not in pre_name:
                                                if pre_name in output_dict:
                                                    output_dict[pre_name] = output_dict[pre_name] + plus_dict[pre_name]
                                                    out_name_list.append(pre_name)
                                                else:
                                                    output_dict[pre_name] = plus_dict[pre_name]
                                                output_data_type_dict[pre_name] = pre_data_type
                                                output_is_required_dict[pre_name] = pre_is_required
                                            att_key = re.findall('{(.*)}', output_conf_tuple_name)[0]
                                            att_np = input_dict[att_key]
                                            for att_set_value in set(att_np.tolist()):
                                                if att_set_value != None:
                                                    tmp_list = []
                                                    tmp_output_conf_tuple_name = output_conf_tuple_name.replace('{' + att_key + '}', str(att_set_value))
                                                    for i, att_value in enumerate(att_np):
                                                        if att_set_value == att_value:
                                                            tmp_list.append(input_np[i])
                                                        else:
                                                            tmp_list.append(None)
                                                    if tmp_output_conf_tuple_name in name_with_att_dict:
                                                        tmp2_list = name_with_att_dict[tmp_output_conf_tuple_name]
                                                        name_with_att_dict[tmp_output_conf_tuple_name] = [tmp2_list[i] if value == None else value for i, value in enumerate(tmp_list)]
                                                    else:
                                                        name_with_att_dict[tmp_output_conf_tuple_name] = tmp_list
                                                    output_data_type_dict[tmp_output_conf_tuple_name] = output_conf_tuple.data_type
                                                    output_is_required_dict[tmp_output_conf_tuple_name] = output_conf_tuple.is_required
                                            plus_dict = {}
                                            pre_name = ''
                                            pre_data_type = ''
                                        else:
                                            if output_conf_tuple_name == pre_name:
                                                if not np.all(input_np == None):
                                                    if output_conf_tuple.is_plus:
                                                        tmp_list = plus_dict[output_conf_tuple_name]
                                                        plus_dict[output_conf_tuple_name] = [None if value == None or tmp_list[i] == None else tmp_list[i] + value for i, value in enumerate(input_np)]
                                                    else:
                                                        plus_dict[output_conf_tuple_name] = input_np.tolist()
                                            else:
                                                plus_dict[output_conf_tuple_name] = input_np.tolist()
                                                if len(pre_name) > 0 and '@' not in pre_name:
                                                    if pre_name in output_dict:
                                                        output_dict[pre_name] = output_dict[pre_name] + plus_dict[pre_name]
                                                        out_name_list.append(pre_name)
                                                    else:
                                                        output_dict[pre_name] = plus_dict[pre_name]
                                                    output_data_type_dict[pre_name] = pre_data_type
                                                    output_is_required_dict[pre_name] = pre_is_required
                                            pre_name = output_conf_tuple_name
                                            pre_data_type = output_conf_tuple.data_type
                                            pre_is_required = output_conf_tuple.is_required
                                if len(pre_name) > 0 and '@' not in pre_name:
                                    if pre_name in output_dict:
                                        output_dict[pre_name] = output_dict[pre_name] + plus_dict[pre_name]
                                        out_name_list.append(pre_name)
                                    else:
                                        output_dict[pre_name] = plus_dict[pre_name]
                                    output_data_type_dict[pre_name] = pre_data_type
                                    output_is_required_dict[pre_name] = pre_is_required
                                ec.codes_release(bufr)
                                for name_with_att in name_with_att_dict:
                                    if name_with_att in output_dict:
                                        output_dict[name_with_att] = output_dict[name_with_att] + name_with_att_dict[name_with_att]
                                    else:
                                        ouput_dict_first_key_values_len = len(output_dict[first_key])
                                        name_with_att_values_len = len(name_with_att_dict[name_with_att])
                                        if ouput_dict_first_key_values_len == name_with_att_values_len:
                                            output_dict[name_with_att] = name_with_att_dict[name_with_att]
                                        else:
                                            output_dict[name_with_att] = [None for i in range(ouput_dict_first_key_values_len - name_with_att_values_len)] + name_with_att_dict[name_with_att]
                                if len(set(output_dict.keys()) - set(out_name_list)) > 0:
                                    ouput_dict_first_key_values_len = len(output_dict[first_key])
                                    for out_name in set(output_dict.keys()) - set(out_name_list):
                                         output_dict[out_name] = output_dict[out_name] + [None for i in range(ouput_dict_first_key_values_len - len(output_dict[out_name]))]
                            except gribapi.errors.PrematureEndOfFileError:
                                break
                            except gribapi.errors.WrongLengthError:
                                break
                            except:
                                traceback.print_exc(file=sys.stderr)
                                break
                if 'datetime' in output_dict:
                    name_list = []
                    field_list = []
                    data_list = []
                    is_required_list = []
                    for output_conf_name in np.sort(np.array(list(output_dict.keys()))):
                        if '@' in output_conf_name:
                            continue
                        if output_conf_name in output_dict and any([False if value == None else True for value in output_dict[output_conf_name]]):
                            if output_data_type_dict[output_conf_name] == 'timestamp':
                                field_list.append(pa.field(output_conf_name, pa.timestamp('ms', tz='utc'), nullable=not output_is_required_dict[output_conf_name]))
                            else:
                                field_list.append(pa.field(output_conf_name, output_data_type_dict[output_conf_name], nullable=not output_is_required_dict[output_conf_name]))
                            if output_data_type_dict[output_conf_name] == 'float16':
                                data_list.append(pa.array(np.array(output_dict[output_conf_name]).astype(np.float16), 'float16'))
                            else:
                                data_list.append(pa.array(output_dict[output_conf_name]))
                            
                            name_list.append(output_conf_name)
                            is_required_list.append(output_is_required_dict[output_conf_name])
                    if len(field_list) <=0 or False not in is_required_list:
                        continue
                    batch = pa.record_batch(data_list, pa.schema(field_list))
                    convert_to_dataset(cccc, output_cat, output_subcat, batch.to_pandas(), out_dir, out_list_file, conf_bufr_arrow_to_dataset_df, debug)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument("--config_bufr_to_arrow", type=str, metavar='conf_bufr_to_arrow.csv', default=pkg_resources.resource_filename(__name__, 'conf_bufr_to_arrow.csv'))
    parser.add_argument("--config_bufr_arrow_to_dataset", type=str, metavar='conf_bufr_arrow_to_dataset.csv', default=pkg_resources.resource_filename(__name__, 'conf_bufr_arrow_to_dataset.csv'))
    parser.add_argument("--debug", action='store_true')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    args = parser.parse_args()
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(args.config_bufr_to_arrow, os.F_OK):
        print('Error', errno, ':', args.config_bufr_to_arrow, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_bufr_arrow_to_dataset, os.F_OK):
        print('Error', errno, ':', args.config_bufr_arrow_to_dataset, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config_bufr_to_arrow):
        print('Error', errno, ':', args.config_bufr_to_arrow, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config_bufr_arrow_to_dataset):
        print('Error', errno, ':', args.config_bufr_arrow_to_dataset, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_bufr_to_arrow, os.R_OK):
        print('Error', errno, ':', args.config_bufr_to_arrow, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_bufr_arrow_to_dataset, os.R_OK):
        print('Error', errno, ':', args.config_bufr_arrow_to_dataset, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        conf_bufr_to_arrow_df = csv.read_csv(args.config_bufr_to_arrow).to_pandas()
        conf_bufr_arrow_to_dataset_df = csv.read_csv(args.config_bufr_arrow_to_dataset).to_pandas()
        convert_to_arrow(input_file_list, conf_bufr_to_arrow_df, args.output_directory, args.output_list_file, conf_bufr_arrow_to_dataset_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
