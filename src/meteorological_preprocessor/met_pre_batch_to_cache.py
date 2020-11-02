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
import numpy as np
import os
import pkg_resources
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pyarrow import csv
from eccodes import *

def is_bufr_matched(in_file, bufr_descriptor, bufr_key_of_not_missing):
    rc = False
    with open(in_file, 'r') as in_file_stream:
        while True:
            bufr = None
            try:
                bufr = codes_bufr_new_from_file(in_file_stream)
            except:
                break
            if bufr is None:
                break
            unexpanded_descriptors = []
            try:
                codes_set(bufr, 'unpack', 1)
                unexpanded_descriptors = codes_get_array(bufr, 'unexpandedDescriptors')
            except:
                break
            descriptor_conf_df = None
            if bufr_descriptor in unexpanded_descriptors:
                if bufr_key_of_not_missing:
                    try:
                        values = codes_get_array(bufr, bufr_key_of_not_missing)
                        if type(values[0]) == str and len(values[0].lstrip().rstrip()) > 0:
                            rc = True
                        elif not np.isnan(values[0]):
                            rc = True
                    except:
                        return False
                else:
                    rc = True
            codes_release(bufr)
    return rc

def get_ttaaii_cccc_ddhhmm_bbb_data_date_list(message, in_file, debug):
    ttaaii_cccc_ddhhmm_bbb_data_date_list = []
    word = ''
    header_num = 0
    message_counter = 0
    data_date = ''
    while message_counter < len(message):
        if message[message_counter] == 10 or message[message_counter] == 13:
            if word:
                ttaaii_cccc_ddhhmm_bbb_data_date_list.append(word)
            break
        elif message[message_counter] == 32:
            if word:
                ttaaii_cccc_ddhhmm_bbb_data_date_list.append(word)
            word = ''
            header_num += 1
        else:
            try:
                word += message[message_counter].to_bytes(1, 'little').decode()
            except:
                return []
        message_counter += 1
    if len(ttaaii_cccc_ddhhmm_bbb_data_date_list) == 3:
        ttaaii_cccc_ddhhmm_bbb_data_date_list.append('')
    if len(ttaaii_cccc_ddhhmm_bbb_data_date_list) == 4:
        in_file_mtime = datetime.utcfromtimestamp(os.path.getmtime(in_file))
        ddhhmm = ttaaii_cccc_ddhhmm_bbb_data_date_list[2]
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
    ttaaii_cccc_ddhhmm_bbb_data_date_list.append(data_date)
    if debug and len(ttaaii_cccc_ddhhmm_bbb_data_date_list) == 5:
        print('Debug', ':', 'ttaaii_cccc_ddhhmm_bbb_data_date =', ttaaii_cccc_ddhhmm_bbb_data_date_list, file=sys.stderr)
    return ttaaii_cccc_ddhhmm_bbb_data_date_list

def get_grib_subdir_list(grib_file):
    warno = 186
    subdir_list = []
    with open(grib_file, 'rb') as grib_file_stream:
        is_grib = True
        while 1:
            try:
                gid = codes_grib_new_from_file(grib_file_stream)
                if gid is None:
                    break
                i_size = codes_get(gid, 'iDirectionIncrementInDegrees')
                j_size = codes_get(gid, 'jDirectionIncrementInDegrees')
                if i_size <= 0 and j_size > 0:
                    i_size = j_size
                elif i_size > 0 and j_size <= 0:
                    j_size = i_size
                subdir_list.append(str(i_size) + '_' + str(j_size) + '_' + str(codes_get(gid, 'latitudeOfFirstGridPointInDegrees')) + '_' + str(codes_get(gid, 'longitudeOfFirstGridPointInDegrees')) + '_' + str(codes_get(gid, 'latitudeOfLastGridPointInDegrees')) + '_' + str(codes_get(gid, 'longitudeOfLastGridPointInDegrees')))
                subdir_list.append(str(codes_get(gid, 'dataDate')).zfill(8) + str(codes_get(gid, 'dataTime')).zfill(4)[0:4])
                codes_release(gid)
            except:
                print('Warning', warno, ':', 'GRIB decode error on', grib_file, 'has occurred. The file is not created', file=sys.stderr)
                is_grib = False
    if not is_grib:
        return []
    return subdir_list

def create_file(in_file, my_cccc, message, start_char4, out_dir, tmp_grib_file, conf_list, debug):
    warno = 187
    in_file_name = os.path.basename(in_file)
    for conf_row in conf_list:
        if re.match(r'' + conf_row.file_name_pattern, in_file_name):
            ttaaii = ''
            cccc = ''
            ddhhmm = ''
            bbb = ''
            data_date = ''
            out_directory_list = []
            out_directory_list.append(out_dir)
            out_directory_list.append(conf_row.access_control)
            ttaaii_cccc_ddhhmm_bbb_data_date_list = []
            if re.match(r'^[A-Z][A-Z][A-Z][A-Z]$', start_char4):
                ttaaii_cccc_ddhhmm_bbb_data_date_list = get_ttaaii_cccc_ddhhmm_bbb_data_date_list(message, in_file, debug)
            if len(ttaaii_cccc_ddhhmm_bbb_data_date_list) == 5:
                ttaaii = ttaaii_cccc_ddhhmm_bbb_data_date_list[0]
                cccc = ttaaii_cccc_ddhhmm_bbb_data_date_list[1]
                ddhhmm = ttaaii_cccc_ddhhmm_bbb_data_date_list[2]
                bbb = ttaaii_cccc_ddhhmm_bbb_data_date_list[3]
                data_date = ttaaii_cccc_ddhhmm_bbb_data_date_list[4]
                out_directory_list.append(cccc)
                out_directory_list.append(conf_row.format)
                out_directory_list.append(conf_row.category)
            elif conf_row.cccc:
                cccc = conf_row.cccc
                out_directory_list.append(cccc)
                out_directory_list.append(conf_row.format)
                out_directory_list.append(conf_row.category)
            if conf_row.cccc and conf_row.cccc != cccc:
                continue
            if conf_row.file_extension == 'txt' and conf_row.text_pattern and not re.search(r'' + conf_row.text_pattern, message.decode("ascii", errors="ignore").replace(ttaaii, '', 1).replace(cccc, '', 1).replace('\r', ' ').replace('\n', ' ')):
                continue
            if conf_row.format == 'bufr' and not np.isnan(conf_row.bufr_descriptor) and not is_bufr_matched(in_file, conf_row.bufr_descriptor, conf_row.bufr_key_of_not_missing):
                continue
            if not re.match(r'^[A-Z][A-Z][A-Z][A-Z]$', cccc):
                print('Warning', warno, ':', 'cccc of', ttaaii, cccc, ddhhmm, bbb, 'on', in_file, 'is invalid. The file is not created', file=sys.stderr)
                return ''
            if conf_row.format == 'grib' or re.match(r'^GRIB$', start_char4):
                subdir_list = get_grib_subdir_list(in_file)
                if len(subdir_list) == 2:
                    out_directory_list.extend(subdir_list)
                    data_date = subdir_list[1][0:8]
                else:
                    return ''
            elif not data_date and re.match(r'^BUFR$', start_char4):
                out_directory_list.append(conf_row.subcategory)
                is_bufr = True
                with open(in_file, 'rb') as bufr_file_stream:
                    while True:
                        try:
                            bufr = codes_bufr_new_from_file(bufr_file_stream)
                            if bufr is None:
                                break
                            codes_set(bufr, 'unpack', 1)
                            year = codes_get_array(bufr, 'typicalYear')[0]
                            month = codes_get_array(bufr, 'typicalMonth')[0]
                            day = codes_get_array(bufr, 'typicalDay')[0]
                            hour = codes_get_array(bufr, 'typicalHour')[0]
                            minute = codes_get_array(bufr, 'typicalMinute')[0]
                            codes_release(bufr)
                            if month > 0 and month < 13 and day > 0 and day < 32 and hour > -1 and hour < 24 and minute > -1 and minute <60:
                                data_date = str(year).zfill(4) + str(month).zfill(2) + str(day).zfill(2)
                                out_directory_list.append(data_date + str(hour).zfill(2) + str(minute).zfill(2))
                            else:
                                is_bufr = False
                                print('Warning', warno, ':', 'BUFR on', in_file, 'is invalid datetime. The file is not created', file=sys.stderr)
                        except:
                            is_bufr = False
                            print('Warning', warno, ':', 'BUFR decode error on', in_file, 'has occurred. The file is not created', file=sys.stderr)
                if not is_bufr:
                    return ''
            if conf_row.format != 'grib' and not re.match(r'^GRIB$', start_char4):
                out_directory_list.append(conf_row.subcategory)
                out_directory_list.append(data_date + ddhhmm[2:6])
            out_directory = '/'.join(out_directory_list)
            os.makedirs(out_directory, exist_ok=True)
            if ttaaii:
                out_file_list = []
                out_file_list.append(out_directory)
                out_file_list.append('/A_')
                out_file_list.append(ttaaii)
                out_file_list.append(cccc)
                out_file_list.append(ddhhmm)
                out_file_list.append(bbb)
                out_file_list.append('_C_')
                out_file_list.append(my_cccc)
                out_file_list.append('_')
                out_file_list.append(datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f'))
                out_file_list.append('.')
                out_file_list.append(conf_row.file_extension)
                out_file = ''.join(out_file_list)
                with open(out_file, 'wb') as out_file_stream:
                    out_file_stream.write(message)
                return out_file
            else:
                out_file_list = []
                out_file_list.append(out_directory)
                out_file_list.append(os.path.basename(in_file))
                out_file = '/'.join(out_file_list)
                if os.path.exists(out_file):
                    if debug:
                        print('Debug', ':', in_file, 'already exists', '. The file is not created', file=sys.stderr)
                    return ''
                else:
                    with open(out_file, 'wb') as out_file_stream:
                        out_file_stream.write(message)
                    return out_file
    print('Warning', warno, ':', in_file, 'is not matched on configuration file. The file is not created', file=sys.stderr)
    return ''

def create_file_from_batch(in_file, my_cccc, message, out_dir, tmp_grib_file, conf_list, debug):
    warno = 188
    ttaaii_cccc_ddhhmm_bbb_data_date_list = get_ttaaii_cccc_ddhhmm_bbb_data_date_list(message, in_file, debug)
    if len(ttaaii_cccc_ddhhmm_bbb_data_date_list) != 5:
        print('Warning', warno, ':', 'header of', ttaaii_cccc_ddhhmm_bbb_data_date_list, 'on', in_file, 'is invalid. The file is not created', file=sys.stderr)
        return ''
    ttaaii = ttaaii_cccc_ddhhmm_bbb_data_date_list[0]
    cccc = ttaaii_cccc_ddhhmm_bbb_data_date_list[1]
    ddhhmm = ttaaii_cccc_ddhhmm_bbb_data_date_list[2]
    bbb = ttaaii_cccc_ddhhmm_bbb_data_date_list[3]
    data_date = ttaaii_cccc_ddhhmm_bbb_data_date_list[4]
    for conf_row in conf_list:
        if re.match(r'' + conf_row.ttaaii_pattern, ttaaii) and re.match(r'' + conf_row.file_name_pattern, os.path.basename(in_file)):
            if conf_row.cccc and conf_row.cccc != cccc:
                continue
            if conf_row.file_extension == 'txt' and conf_row.text_pattern and not re.search(r'' + conf_row.text_pattern, message.decode("ascii", errors="ignore").replace(ttaaii, '', 1).replace(cccc, '', 1).replace('\r', ' ').replace('\n', ' ')):
                continue
            if conf_row.format == 'bufr' and not np.isnan(conf_row.bufr_descriptor) and not is_bufr_matched(in_file, conf_row.bufr_descriptor, conf_row.bufr_key_of_not_missing):
                continue
            if not re.match(r'^[A-Z][A-Z][A-Z][A-Z]$', cccc):
                print('Warning', warno, ':', 'cccc of', ttaaii, cccc, ddhhmm, bbb, 'on', in_file, 'is invalid. The file is not created', file=sys.stderr)
                return ''
            if data_date and re.match(r'([0-1][0-9]|2[0-4])', ddhhmm[2:4]) and re.match(r'[0-5][0-9]', ddhhmm[4:6]):
                out_directory_list = []
                out_directory_list.append(out_dir)
                out_directory_list.append(conf_row.access_control)
                out_directory_list.append(cccc)
                out_directory_list.append(conf_row.format)
                out_directory_list.append(conf_row.category)
                if conf_row.format == 'grib':
                    with open(tmp_grib_file, 'wb') as tmp_grib_file_stream:
                            tmp_grib_file_stream.write(message)
                    subdir_list = get_grib_subdir_list(in_file)
                    if len(subdir_list) == 2:
                        out_directory_list.extend(subdir_list)
                        data_date = subdir_list[1][0:8]
                    else:
                        return ''
                else:
                    out_directory_list.append(conf_row.subcategory)
                    out_directory_list.append(data_date + ddhhmm[2:6])
                out_directory = '/'.join(out_directory_list)
                os.makedirs(out_directory, exist_ok=True)
                out_file_list = []
                out_file_list.append(out_directory)
                out_file_list.append('/A_')
                out_file_list.append(ttaaii)
                out_file_list.append(cccc)
                out_file_list.append(ddhhmm)
                out_file_list.append(bbb)
                out_file_list.append('_C_')
                out_file_list.append(my_cccc)
                out_file_list.append('_')
                out_file_list.append(datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f'))
                out_file_list.append('.')
                out_file_list.append(conf_row.file_extension)
                out_file = ''.join(out_file_list)
                with open(out_file, 'wb') as out_file_stream:
                    out_file_stream.write(message)
                return out_file
            else:
                print('Warning', warno, ':', 'ddhhmm of', ttaaii, cccc, ddhhmm, bbb, 'on', in_file, 'is invalid. The file is not created', file=sys.stderr)
                return ''
    print('Warning', warno, ':', in_file, 'is not matched on configuration file. The file is not created', file=sys.stderr)
    return ''

def convert_to_cache(my_cccc, input_file_list, out_dir, out_list_file, tmp_grib_file, conf_list, debug):
    warno = 189
    for in_file in input_file_list:
        if debug:
            print('Debug', ':', 'in_file =', in_file, file=sys.stderr)
        with open(in_file, 'rb') as in_file_stream:
            batch_type = 0
            message_length = 0
            start_byte4 = None
            start_char4 = None
            try:
                start_byte4 = in_file_stream.read(4)
                if len(start_byte4) < 4:
                    break
                start_char4 = start_byte4.decode()
            except:
                print('Warning', warno, ':', 'The start 4 bytes of', in_file, 'are not strings.', file=sys.stderr)
            while start_char4:
                if debug:
                    print('Debug', ':', 'start_char4 =', start_char4, file=sys.stderr)
                message = bytearray()

                if re.match(r'\d\d\d\d', start_char4):
                    batch_type = 1
                    message_length = int(start_char4 + in_file_stream.read(4).decode())
                    try:
                        if message_length == 0:
                            break
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
                    except:
                        print('Warning', warno, ':', 'The bytes of message length on', in_file, 'are not strings.', file=sys.stderr)
                        break
                elif start_char4 == '####':
                    try:
                        batch_type = 2
                        in_file_stream.read(3) # skip '018'
                        message_length = int(in_file_stream.read(6).decode())
                        in_file_stream.read(5) # skip ####\n
                    except:
                        print('Warning', warno, ':', 'The bytes of message length on', in_file, 'are not strings.', file=sys.stderr)
                        break
                elif start_char4 == '****':
                    try:
                        batch_type = 3
                        message_length = int(in_file_stream.read(10).decode())
                        in_file_stream.read(5) # skip ****\n
                    except:
                        print('Warning', warno, ':', 'The bytes of message length on', in_file, 'are not strings.', file=sys.stderr)
                        break
                else:
                    try:
                        message.extend(start_char4.encode())
                        message.extend(in_file_stream.read())
                    except:
                        print('Warning', warno, ':', 'can not encode or read', in_file, file=sys.stderr)
                        break
                    out_file = create_file(in_file, my_cccc, message, start_char4, out_dir, tmp_grib_file, conf_list, debug)
                    if out_file:
                        print(out_file, file=out_list_file)
                    break
                if message_length <= 0:
                    if debug:
                        print('Debug', ':', 'The message length of', in_file, 'is invalid. (<=0)', file=sys.stderr)
                    break
                if debug:
                    print('Debug', ':', 'batch_type =', batch_type, ', message_length =', message_length, file=sys.stderr)
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
                out_file = create_file_from_batch(in_file, my_cccc, message, out_dir, tmp_grib_file, conf_list, debug)
                if out_file:
                    print(out_file, file=out_list_file)
                try:
                    byte4 = in_file_stream.read(4)
                    if len(byte4) < 4:
                        break
                    start_char4 = byte4.decode()
                except:
                    start_char4 = None
                    print('Warning', warno, ':', 'The start 4 bytes of the message on', in_file, 'are not strings.', file=sys.stderr)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_directory_or_list_file', type=str, metavar='input_directory_or_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument('--tmp_grib_file', type=str, metavar='tmp_grib_file', default='tmp_grib_file.bin')
    parser.add_argument("--config", type=str, metavar='conf_batch_to_cache.csv', default=pkg_resources.resource_filename(__name__, 'conf_batch_to_cache.csv'))
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    input_file_list = []
    if not re.match(r'^[A-Z]{4}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z]{4}$).', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_directory_or_list_file, os.F_OK):
        print('Error', errno, ':', args.input_directory_or_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.access(args.config, os.F_OK):
        print('Error', errno, ':', args.config, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if os.path.isdir(args.input_directory_or_list_file) and os.access(args.input_directory_or_list_file, os.R_OK) and os.access(args.input_directory_or_list_file, os.X_OK):
        in_dir_entry_list = [f for f in os.scandir(args.input_directory_or_list_file) if os.path.isfile(f) and os.access(f, os.R_OK) and not re.match(r'(^.*\.tmp$|^\..*$)', f.name) and os.path.getsize(f) > 4]
        input_file_list = [in_dir_entry.path for in_dir_entry in sorted(in_dir_entry_list, key=os.path.getmtime)]
    elif os.path.isfile(args.input_directory_or_list_file) and os.access(args.input_directory_or_list_file, os.R_OK):
        with open(args.input_directory_or_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
    else:
        print('Error', errno, ':', args.input_directory_or_list_file, 'is not directory/file/readable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config):
        print('Error', errno, ':', args.config, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config, os.R_OK):
        print('Error', errno, ':', args.config, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        conf_list = list(csv.read_csv(args.config).to_pandas().itertuples())
        convert_to_cache(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, args.tmp_grib_file, conf_list, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
