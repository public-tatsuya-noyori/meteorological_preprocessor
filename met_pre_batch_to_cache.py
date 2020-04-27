#!/usr/bin/env python3
#
# Copyright 2020 Japan Meteorological Agency.
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
import os
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pyarrow import csv

def create_cache(my_cccc, conf_df, in_file_name, ttaaii, cccc, ddhhmm, ccc, out_dir, message, debug):
    warno = 188
    out_file_name = []
    out_directory_path = []
    for conf_row in conf_df.itertuples():
        if re.match(conf_row.file_name_pattern, in_file_name) and re.match(conf_row.cccc_pattern, cccc) and re.match(conf_row.ttaaii_pattern, ttaaii):
            if not re.match(r'^[A-Z][A-Z][A-Z][A-Z]$', cccc):
                print('Warning', warno, ':', 'cccc of', ttaaii, cccc, ddhhmm, ccc, 'is invalid. The file is not created', file=sys.stderr)
                return ''
            data_date = ''
            now = datetime.now(timezone.utc)
            if ddhhmm[0:2] == now.strftime('%d'):
                data_date = now.strftime('%Y%m%d')
            else:
                for timedelta_day in range(1, 28):
                    data_date = (now + timedelta(days=-timedelta_day)).strftime('%Y%m%d')
                    if ddhhmm[0:2] == data_date[6:8]:
                        break
                if not data_date:
                    for timedelta_day in range(1, 7):
                        data_date = (now + timedelta(days=timedelta_day)).strftime('%Y%m%d')
                        if ddhhmm[0:2] == data_date[6:8]:
                            break
            if data_date and re.match(r'([0-1][0-9]|2[0-4])', ddhhmm[2:4]) and re.match(r'[0-5][0-9]', ddhhmm[4:6]):
                out_directory_path.append(out_dir)
                out_directory_path.append(conf_row.access_control)
                out_directory_path.append(conf_row.file_format)
                out_directory_path.append(conf_row.data_category)
                out_directory_path.append(conf_row.data_subcategory)
                out_directory_path.append(cccc)
                out_directory_path.append(data_date + ddhhmm[2:4])
                directory_path = '/'.join(out_directory_path)
                os.makedirs(directory_path, exist_ok=True)
                out_file_name.append('A_')
                out_file_name.append(ttaaii)
                out_file_name.append(cccc)
                out_file_name.append(ddhhmm)
                out_file_name.append(ccc)
                out_file_name.append('_C_')
                out_file_name.append(my_cccc)
                out_file_name.append('_')
                file_name_prefix = ''.join(out_file_name)
                for file_counter in range(0, 999):
                    file_path_list = []
                    file_path_list.append(directory_path)
                    file_path_list.append('/')
                    file_path_list.append(file_name_prefix)
                    file_path_list.append(str(file_counter))
                    file_path_list.append('.')
                    file_path_list.append(conf_row.file_extension)
                    file_path = ''.join(file_path_list)
                    if debug:
                        print('Debug', ':', 'file_path =', file_path, file=sys.stderr)
                    if os.access(file_path, os.F_OK):
                        with open(file_path, 'rb') as cmp_f:
                            if message == cmp_f.read():
                                if debug:
                                    print('Debug', ':', ttaaii, cccc, ddhhmm, ccc, 'is duplicate content. The file is not newly created.', file=sys.stderr)
                                return ''
                    else:
                        with open(file_path, 'wb') as out_f:
                            out_f.write(message)
                            return file_path

                print('Warning', warno, ':', 'There are 999 files with the same', ttaaii, cccc, ddhhmm, ccc, '. The file is not newly created', file=sys.stderr)
            else:
                print('Warning', warno, ':', 'ddhhmm of', ttaaii, cccc, ddhhmm, ccc, 'is invalid. The file is not created', file=sys.stderr)
    return ''

def convert_to_cache(in_dir, out_dir, my_cccc, out_cached_list_file, conf_df, debug):
    warno = 189
    cached_file_list = []
    in_files = [f for f in os.scandir(in_dir) if os.path.isfile(f) and os.access(f, os.R_OK) and not re.match(r'(^.*\.tmp$|^\..*$)', f.name)]
    for in_file in sorted(in_files, key=os.path.getmtime):
        with open(in_file, 'rb') as in_f:
            batch_type = 0
            message_size = 0
            try:
                start_char4 = in_f.read(4).decode()
            except:
                start_char4 = None
                print('Warning', warno, ':', 'The first 4 characters of', in_f.name, 'are not strings.', file=sys.stderr)
                pass
            while start_char4:
                try:
                    if re.match(r'\d\d\d\d',start_char4):
                        batch_type = 1
                        message_size = int(start_char4 + in_f.read(4).decode())
                        in_f.read(12) # skip
                    elif start_char4 == '####':
                        batch_type = 2
                        in_f.read(3) # skip '018'
                        message_size = int(in_f.read(6).decode())
                        in_f.read(5) # skip ####\n
                    elif start_char4 == '****':
                        batch_type = 3
                        message_size = int(in_f.read(10).decode())
                        in_f.read(5) # skip ****\n
                    else:
                        print('Warning', warno, ':', 'The first 4 characters of', in_f.name, 'are not strings of batch ftp file.', file=sys.stderr)
                        break
                except:
                    print('Warning', warno, ':', 'The message size of', in_f.name, 'is not strings.', file=sys.stderr)
                    break
                message = None
                if batch_type == 1:
                    message = bytearray(in_f.read(message_size - 12))
                elif batch_type == 2 or batch_type == 3:
                    message = bytearray(in_f.read(message_size))
                message_counter = len(message) - 1
                while message_counter > -1:
                    if message[message_counter] == 10 or message[message_counter] == 13 or message[message_counter] == 32:
                        message.pop(message_counter)
                    else:
                        break
                    message_counter -= 1
                message_counter = 0
                while message_counter < len(message):
                    if message[0] == 10 or message[0] == 13 or message[0] == 32:
                        message.pop(0)
                    else:
                        break
                    message_counter += 1
                ttaaii = ''
                cccc = ''
                ddhhmm = ''
                ccc = ''
                head_num = 0
                message_counter = 0
                while message_counter < len(message):
                    if message[message_counter] == 32:
                        head_num += 1
                    else:
                        if head_num == 2 and len(ddhhmm) == 6:
                            break
                        if head_num == 0:
                            ttaaii += message[message_counter].to_bytes(1, 'big').decode()
                        elif head_num == 1:
                            cccc += message[message_counter].to_bytes(1, 'big').decode()
                        elif head_num == 2:
                            ddhhmm += message[message_counter].to_bytes(1, 'big').decode()
                        elif head_num == 3:
                            ccc += message[message_counter].to_bytes(1, 'big').decode()
                        if head_num == 3 and len(ccc) == 3:
                            break
                    message_counter += 1
                if batch_type == 1:
                    try:
                        in_f.read(1) # skip
                    except:
                        if debug:
                            print('Debug', ':', 'can not skip footer of a message.', file=sys.stderr)
                        pass
                if debug:
                    print('Debug', ':', 'batch_type =', batch_type, ', message_size =', message_size, ', ttaaii =', ttaaii, ', cccc =', cccc, ', ddhhmm =', ddhhmm, ', ccc =', ccc, file=sys.stderr)
                cached_file = create_cache(my_cccc, conf_df, in_f.name, ttaaii, cccc, ddhhmm, ccc, out_dir, message, debug)
                if cached_file:
                    cached_file_list.append(cached_file)
                try:
                    byte4 = in_f.read(4)
                    if len(byte4) < 4:
                        break
                    start_char4 = byte4.decode()
                except:
                    start_char4 = None
                    print('Warning', warno, ':', 'The first 4 characters of', in_f.name, 'are not strings.', file=sys.stderr)
    if len(cached_file_list) > 0:
        with open(out_cached_list_file, 'w') as out_cached_list:
            out_cached_list.write('\n'.join(cached_file_list))
            if debug:
                print('Debug', ':', len(cached_file_list), 'files have been saved as cache.', file=sys.stderr)
    else:
        with open(out_cached_list_file, 'w') as out_cached_list:
            out_cached_list.write('\n'.join(cached_file_list))
            if debug:
                print('Debug', ':', 'No files have been saved as cache.', file=sys.stderr)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_batch_file_directory', type=str, metavar='input_batch_file_directory')
    parser.add_argument('output_cache_directory', type=str, metavar='output_cache_directory')
    parser.add_argument('output_cached_file_list', type=str, metavar='output_cached_file_list')
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument("--config", type=str, metavar='config/batch_to_cache.csv', default='config/batch_to_cache.csv')
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if not os.access(args.input_batch_file_directory, os.F_OK):
        print('Error', errno, ':', args.input_batch_file_directory, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_cache_directory, os.F_OK):
        print('Error', errno, ':', args.output_cache_directory, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_cached_file_list, os.F_OK):
        print('Error', errno, ':', args.output_cached_file_list, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config, os.F_OK):
        print('Error', errno, ':', args.config, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.input_batch_file_directory):
        print('Error', errno, ':', args.input_batch_file_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_cache_directory):
        print('Error', errno, ':', args.output_cache_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.output_cached_file_list):
        print('Error', errno, ':', args.output_cached_file_list, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config):
        print('Error', errno, ':', args.config, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.input_batch_file_directory, os.R_OK) and os.access(args.input_batch_file_directory, os.W_OK) and os.access(args.input_batch_file_directory, os.X_OK)):
        print('Error', errno, ':', args.input_batch_file_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_cache_directory, os.R_OK) and os.access(args.output_cache_directory, os.W_OK) and os.access(args.output_cache_directory, os.X_OK)):
        print('Error', errno, ':', args.output_cache_directory, 'is not readable and writable and executable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_cached_file_list, os.R_OK) and os.access(args.output_cached_file_list, os.W_OK)):
        print('Error', errno, ':', args.output_cached_file_list, 'is not readable/writable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config, os.R_OK):
        print('Error', errno, ':', args.config, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        conf_pa = csv.read_csv(args.config)
        conf_df = conf_pa.to_pandas()
        convert_to_cache(args.input_batch_file_directory, args.output_cache_directory, args.my_cccc, args.output_cached_file_list, conf_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
