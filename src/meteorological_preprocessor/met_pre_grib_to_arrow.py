#!/usr/bin/env python3
#
# Copyright 2020-2021 Japan Meteorological Agency.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
#   Tatsuya Noyori - Japan Meteorological Agency - https://www.jma.go.jp
#

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
from pyarrow import csv, feather
from eccodes import *

def convert_to_arrow(my_cccc, in_file_list, out_dir, out_list_file, conf_df, is_location, debug):
    warno = 189
    out_arrows = []
    now = datetime.utcnow()
    create_datetime_directory_list = ['C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)]
    create_datetime_directory = ''.join(create_datetime_directory_list)
    cccc_set = set([re.sub('^.*/', '', re.sub('/grib/.*$', '', in_file)) for in_file in in_file_list])
    cat_subcat_set = set([re.search(r'^[^/]*/[^/]*/', re.sub('^.*/grib/', '', in_file)).group().rstrip('/') for in_file in in_file_list])
    for cccc in cccc_set:
        for cat_subcat in cat_subcat_set:
            keys = ['stepRange', 'typeOfLevel', 'level', 'shortName']
            missingValue = -3.402823e+38
            for in_file in in_file_list:
                property_dict = {}
                ft_list = []
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
                datetime_directory = re.sub('/.*$', '', re.sub('^.*/' + cccc + '/grib/' + cat_subcat + '/', '', in_file))
                with open(in_file, 'r') as in_file_stream:
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
                                gid = codes_new_from_index(iid)
                                if gid is None:
                                    break
                                codes_set(gid, 'missingValue', missingValue)
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
                                        if key == 'stepRange' or key == 'level':
                                            target_conf_df = target_conf_df[(target_conf_df[key] == int(value))]
                                        else:
                                            target_conf_df = target_conf_df[(target_conf_df[key] == value)]
                                codes_keys_iterator_delete(iterid)
                                message_np = np.array([])
                                for conf_row in target_conf_df.itertuples():
                                    ft = codes_get(gid, 'stepRange')
                                    if not ft in ft_list:
                                        ft_list.append(ft)
                                    property_dict[(conf_row.category, conf_row.subcategory, conf_row.stepRange, conf_row.typeOfLevel, conf_row.level, conf_row.shortName, ft)] = np.array(codes_get_values(gid))
                                if is_location:
                                    iterid = codes_grib_iterator_new(gid, 0)
                                    lat_list = []
                                    lon_list = []
                                    while True:
                                        latitude_longitude_value = codes_grib_iterator_next(iterid)
                                        if not latitude_longitude_value:
                                            break
                                        else:
                                            lat_list.append(latitude_longitude_value[0])
                                            lon_list.append(latitude_longitude_value[1])
                                    codes_grib_iterator_delete(iterid)
                                    out_directory_list = [out_dir, cccc, 'grib_to_arrow', conf_row.category, conf_row.subcategory]
                                    out_directory = '/'.join(out_directory_list)
                                    os.makedirs(out_directory, exist_ok=True)
                                    out_file_list = [out_directory, '/location.feather']
                                    out_file = ''.join(out_file_list)
                                    with open(out_file, 'bw') as out_f:
                                        location_batch = pa.record_batch([pa.array(lat_list, 'float32'), pa.array(lon_list, 'float32')], names=['latitude [degree]', 'longitude [degree]'])
                                        location_table = pa.Table.from_batches([location_batch])
                                        feather.write_feather(location_table, out_f, compression='zstd')
                                codes_release(gid)
                    except:
                        print('Warning', warno, ':', in_file, 'is invalid grib.', file=sys.stderr)
                if len(property_dict) > 0:
                    now = datetime.utcnow()
                    create_datetime_directory_list = ['C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)]
                    create_datetime_directory = ''.join(create_datetime_directory_list)
                    for conf_row in conf_df[(conf_df['category'] == cat) & (conf_df['subcategory'] == subcat)].itertuples():
                        for ft in ft_list:
                            if len(property_dict[(conf_row.category, conf_row.subcategory, conf_row.stepRange, conf_row.typeOfLevel, conf_row.level, conf_row.shortName, ft)]) > 0:
                                property_data = []
                                value_list = property_dict[(conf_row.category, conf_row.subcategory, conf_row.stepRange, conf_row.typeOfLevel, conf_row.level, conf_row.shortName, ft)]
                                id_list = range(len(value_list))
                                property_name_list = [conf_row.name]
                                property_data.append(pa.array(value_list, conf_row.datatype))
                                out_directory_list = [out_dir, cccc, 'grib_to_arrow', conf_row.category, conf_row.subcategory, datetime_directory, create_datetime_directory, ft]
                                out_directory = '/'.join(out_directory_list)
                                os.makedirs(out_directory, exist_ok=True)
                                out_file_list = [out_directory, '/', conf_row.output, '.feather']
                                out_file = ''.join(out_file_list)
                                with open(out_file, 'bw') as out_f:
                                    property_batch = pa.record_batch(property_data, names=property_name_list)
                                    property_table = pa.Table.from_batches([property_batch])
                                    feather.write_feather(property_table, out_f, compression='zstd')
                                    print(out_file, file=out_list_file)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--location", action='store_true')
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
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, conf_df, args.location, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
