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
import pkg_resources
import re
import sys
import traceback
from datetime import datetime, timedelta
from pyarrow import csv

def create_file(in_file, start_char4, out_dir, conf_list, debug):
    warno = 187
    for conf_row in conf_list:
        if re.match(conf_row.file_name_pattern, os.path.basename(in_file)):
            cccc = conf_row.file_name_pattern.rstrip('^$')
            if re.match('^BUFR$', start_char4) or re.match('^[IJ][A-Z][A-Z][A-Z]$', start_char4):
                print('Info', ':', 'Not implemented yet', file=sys.stderr)
            elif re.match('^GRIB$', start_char4) or re.match('^H[A-Z][A-Z][A-Z]$', start_char4):
                print('Info', ':', 'Not implemented yet', file=sys.stderr)
            else:
                print('Warning', warno, ':', 'The first 4 bytes of', in_file, 'is not matched with BUFR or GRIB. The file is not created', file=sys.stderr)
        else:
            print('Warning', warno, ':', in_file, 'is not matched on configuration file. The file is not created', file=sys.stderr)
    return ''

def create_file_with_header(in_file, ttaaii, cccc, ddhhmm, bbb, message, out_dir, conf_list, debug):
    warno = 188
    for conf_row in conf_list:
        if re.match(conf_row.file_name_pattern, os.path.basename(in_file)) and re.match(conf_row.cccc_pattern, cccc) and re.match(conf_row.ttaaii_pattern, ttaaii):
            if not re.match(r'^[A-Z][A-Z][A-Z][A-Z]$', cccc):
                print('Warning', warno, ':', 'cccc of', ttaaii, cccc, ddhhmm, bbb, 'is invalid. The file is not created', file=sys.stderr)
                return ''
            data_date = ''
            in_file_mtime = datetime.utcfromtimestamp(os.path.getmtime(in_file))
            if ddhhmm[0:2] == in_file_mtime.strftime('%d'):
                data_date = in_file_mtime.strftime('%Y%m%d')
            else:
                for timedelta_day in range(1, 28):
                    data_date = (in_file_mtime + timedelta(days=-timedelta_day)).strftime('%Y%m%d')
                    if ddhhmm[0:2] == data_date[6:8]:
                        break
                if not data_date:
                    for timedelta_day in range(1, 7):
                        data_date = (in_file_mtime + timedelta(days=timedelta_day)).strftime('%Y%m%d')
                        if ddhhmm[0:2] == data_date[6:8]:
                            break
            if data_date and re.match(r'([0-1][0-9]|2[0-4])', ddhhmm[2:4]) and re.match(r'[0-5][0-9]', ddhhmm[4:6]):
                out_directory_list = []
                out_directory_list.append(out_dir)
                out_directory_list.append(conf_row.access_control)
                out_directory_list.append(conf_row.format)
                out_directory_list.append(conf_row.category)
                out_directory_list.append(conf_row.subcategory)
                out_directory_list.append(cccc)
                out_directory_list.append(data_date[0:4])
                out_directory_list.append(data_date[4:8])
                out_directory_list.append(ddhhmm[2:6])
                out_directory = '/'.join(out_directory_list)
                os.makedirs(out_directory, exist_ok=True)
                out_file_name_prefix_list = []
                out_file_name_prefix_list.append(ttaaii)
                for out_file_ext_counter in range(0, 999):
                    out_file_list = []
                    out_file_list.append(out_directory)
                    out_file_list.append('/')
                    out_file_list.append(''.join(out_file_name_prefix_list))
                    if out_file_ext_counter != 0:
                        out_file_list.append('_')
                        out_file_list.append(str(out_file_ext_counter))
                    out_file_list.append('.')
                    out_file_list.append(conf_row.file_extension)
                    out_file = ''.join(out_file_list)
                    if debug:
                        print('Debug', ':', 'file_path =', out_file, file=sys.stderr)
                    if os.access(out_file, os.F_OK):
                        with open(out_file, 'rb') as out_file_stream:
                            if message == out_file_stream.read():
                                if debug:
                                    print('Debug', ':', ttaaii, cccc, ddhhmm, bbb, 'is duplicate content. The file is not created.', file=sys.stderr)
                                return ''
                    else:
                        with open(out_file, 'wb') as out_file_stream:
                            out_file_stream.write(message)
                            return out_file

                print('Warning', warno, ':', 'There are 999 files with the same', ttaaii, cccc, ddhhmm, 'of', in_file, '. The file is not created', file=sys.stderr)
            else:
                print('Warning', warno, ':', 'ddhhmm of', ttaaii, cccc, ddhhmm, bbb, 'in', in_file, 'is invalid. The file is not created', file=sys.stderr)
    return ''

def convert_to_cache(in_dir, out_dir, out_list_file, conf_list, debug):
    warno = 189
    out_file_counter = 0
    in_dir_entry_list = [f for f in os.scandir(in_dir) if os.path.isfile(f) and os.access(f, os.R_OK) and not re.match(r'(^.*\.tmp$|^\..*$)', f.name)]
    for in_dir_entry in sorted(in_dir_entry_list, key=os.path.getmtime):
        in_file = in_dir_entry.path
        with open(in_file, 'rb') as in_file_stream:
            batch_type = 0
            message_length = 0
            try:
                start_char4 = in_file_stream.read(4).decode()
            except:
                start_char4 = None
                print('Warning', warno, ':', 'The first 4 bytes of', in_file, 'are not strings.', file=sys.stderr)
                pass
            while start_char4:
                try:
                    if re.match(r'\d\d\d\d',start_char4):
                        batch_type = 1
                        message_length = int(start_char4 + in_file_stream.read(4).decode())
                        format_identifier = int(in_file_stream.read(2).decode())
                        if format_identifier == 0:
                            in_file_stream.read(10) # skip
                            message_length -= 10
                        elif format_identifier == 1:
                            in_file_stream.read(3) # skip
                            message_length -= 3
                        else:
                            print('Warning', warno, ':', 'The format identifier of', in_file, 'is not 00 or 01.', file=sys.stderr)
                            break
                    elif start_char4 == '####':
                        batch_type = 2
                        in_file_stream.read(3) # skip '018'
                        message_length = int(in_file_stream.read(6).decode())
                        in_file_stream.read(5) # skip ####\n
                    elif start_char4 == '****':
                        batch_type = 3
                        message_length = int(in_file_stream.read(10).decode())
                        in_file_stream.read(5) # skip ****\n
                    else:
                        out_file = create_file(in_file, start_char4, out_dir, conf_list, debug)
                        if out_file:
                            print(out_file, file=out_list_file)
                            out_file_counter += 1
                        break
                    if message_length <= 0:
                        if debug:
                            print('Debug', ':', 'The message length of ', in_file, 'is invalid. (<=0)', bbb, file=sys.stderr)
                        break
                except:
                    print('Warning', warno, ':', 'The bytes of message length in', in_file, 'are not strings.', file=sys.stderr)
                    break
                message = None
                if batch_type == 1:
                    message = bytearray(in_file_stream.read(message_length))
                elif batch_type == 2 or batch_type == 3:
                    message = bytearray(in_file_stream.read(message_length))
                message_counter = len(message) - 1
                while message_counter > -1:
                    if message[message_counter] == 3 or message[message_counter] == 10 or message[message_counter] == 13 or message[message_counter] == 32:
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
                bbb = ''
                header_num = 0
                message_counter = 0
                while message_counter < len(message):
                    if message[message_counter] == 10 or message[message_counter] == 13:
                        break
                    elif message[message_counter] == 32:
                        header_num += 1
                    else:
                        if header_num == 0:
                            ttaaii += message[message_counter].to_bytes(1, 'little').decode()
                        elif header_num == 1:
                            cccc += message[message_counter].to_bytes(1, 'little').decode()
                        elif header_num == 2:
                            ddhhmm += message[message_counter].to_bytes(1, 'little').decode()
                        elif header_num == 3:
                            bbb += message[message_counter].to_bytes(1, 'little').decode()
                    message_counter += 1
                if debug:
                    print('Debug', ':', 'in_file =', in_file, 'batch_type =', batch_type, 'message_length =', message_length, 'ttaaii =', ttaaii, 'cccc =', cccc, 'ddhhmm =', ddhhmm, 'bbb =', bbb, file=sys.stderr)
                out_file = create_file_with_header(in_file, ttaaii, cccc, ddhhmm, bbb, message, out_dir, conf_list, debug)
                if out_file:
                    print(out_file, file=out_list_file)
                    out_file_counter += 1
                try:
                    byte4 = in_file_stream.read(4)
                    if len(byte4) < 4:
                        break
                    start_char4 = byte4.decode()
                except:
                    start_char4 = None
                    print('Warning', warno, ':', 'The first 4 bytes of the message in', in_file.name, 'are not strings.', file=sys.stderr)
    print('Info', ':', out_file_counter, 'files have been saved.', file=sys.stderr)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_directory', type=str, metavar='input_directory')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--config", type=str, metavar='conf_batch_to_cache.csv', default=pkg_resources.resource_filename(__name__, 'conf_batch_to_cache.csv'))
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if not os.access(args.input_directory, os.F_OK):
        print('Error', errno, ':', args.input_directory, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(args.config, os.F_OK):
        print('Error', errno, ':', args.config, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.input_directory):
        print('Error', errno, ':', args.input_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config):
        print('Error', errno, ':', args.config, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.input_directory, os.R_OK) and os.access(args.input_directory, os.X_OK)):
        print('Error', errno, ':', args.input_directory, 'is not readable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config, os.R_OK):
        print('Error', errno, ':', args.config, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        conf_list = list(csv.read_csv(args.config).to_pandas().itertuples())
        convert_to_cache(args.input_directory, args.output_directory, args.output_list_file, conf_list, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
