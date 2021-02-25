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

def get99LaLaLa(token):
    if len(token) != 5:
        return []

def getYYGGiw(token, dt_str):
    r = []
    if len(token) != 5:
        return []
    if not dt_str[6:10] == token[0:4]:
        return []
    else:
        r.append(datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), 0, 0, tzinfo=timezone.utc))
    if token[4] == '0' or token[4] == '1':
        r.append(1.0)
    elif token[4] == '3' or token[4] == '4':
        r.append(5.14444)
    else:
        r.append(0.0)
    return r

def parse(cccc, cat, subcat, in_file, message, dt_str, debug):
    warno = 188
    an_dict = {}
    text = re.sub(' +', ' ', message.decode("ascii", errors="ignore").replace('\r', ' ').replace('\n', ' '))
    out_subcat = ''
    if cat == 'surface':
        if subcat == 'synop' or subcat == 'ship' or subcat == 'synop_mobil':
            if not re.search(r' (AAXX [0-9][0-9][0-9][0-9][0-9]|BBXX|OOXX) ', text):
                print('Warning', warno, ':', in_file, 'does not match " (AAXX [0-9][0-9][0-9][0-9][0-9]|BBXX|OOXX) ".', file=sys.stderr)
                return {}
            text = re.sub(' *\n *', '\n', re.sub('( (AAXX [0-9][0-9][0-9][0-9][0-9]|BBXX|OOXX) )', r'\n\1\n', text.replace('=', '\n')))
            if debug:
                print('Debug', ':', text, file=sys.stderr)
            for line_num, line in enumerate(text.split('\n')):
                if line_num == 1:
                    if not re.search(r'^(AAXX [0-9][0-9][0-9][0-9][0-9]|BBXX|OOXX)$', line):
                        print('Warning', warno, ':', 'The second line of', in_file, 'does not match "(AAXX [0-9][0-9][0-9][0-9][0-9]|BBXX|OOXX)".', file=sys.stderr)
                        return {}
                    line_token_list = line.split(' ')
                    if line_token_list[0] == 'BBXX':
                        out_subcat = 'ship'
                    elif line_token_list[0] == 'OOXX':
                        out_subcat = 'synop_mobil'
                    else:
                        out_subcat = 'synop'
                        datetime_wind_multiply = getYYGGiw(line_token_list[1], dt_str)
                        if len(datetime_wind_multiply) != 2:
                            print('Warning', warno, ':', in_file, 'does not have valid datetime_wind_multiply.', file=sys.stderr)
                            return {}
                elif line_num > 1:
                    line_token_list = line.split(' ')
                    if out_subcat == 'ship':
                        location = line_token_list[0]
                        datetime_wind_multiply = getYYGGiw(line_token_list[1], dt_str)



                    print(line, file=sys.stdout)
                    


def convert_to_arrow(my_cccc, in_file_list, out_dir, out_list_file, debug):
    warno = 189
    out_arrows = []
    now = datetime.utcnow()
    create_datetime_directory_list = ['C_', my_cccc, '_', str(now.year).zfill(4), str(now.month).zfill(2), str(now.day).zfill(2), str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)]
    create_datetime_directory = ''.join(create_datetime_directory_list)
    cccc_set = set([re.sub('^.*/', '', re.sub('/alphanumeric/.*$', '', in_file)) for in_file in in_file_list])
    cat_subcat_set = set([re.search(r'^[^/]*/[^/]*/', re.sub('^.*/alphanumeric/', '', in_file)).group().rstrip('/') for in_file in in_file_list])
    for cccc in cccc_set:
        for cat_subcat in cat_subcat_set:
            cat = re.sub('/.*$', '', cat_subcat)
            subcat = re.sub('^.*/', '', cat_subcat)
            datatype_dict = {}
            output_property_dict = {}
            property_dict = {}
            for in_file in in_file_list:
                match = re.search(r'^.*/' + cccc + '/alphanumeric/' + cat_subcat + '/.*$', in_file)
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
                message = bytearray()
                with open(in_file, 'rb') as in_file_stream:
                    if debug:
                        print('Debug', ':', in_file, file=sys.stderr)
                    message = in_file_stream.read()
                dt_str = re.sub('/.*$', '',  re.sub('^.*/' + cat_subcat + '/', '', in_file))
                parse(cccc, cat, subcat, in_file, message, dt_str, debug)



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
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
