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

dt_name = 'datetime'
lat_name = 'latitude [degree]'
lon_name = 'longitude [degree]'
ship_location_name = 'ship identifier'
synop_location_name = 'station number'
pressure_reduced_to_msl_name = 'pressure reduced to MSL [Pa]'
temperature = 'temperature [K]'
dewpoint_temperature = 'dewpoint temperature [K]'


def geta3hhh(token):
    if token[0:1] == '1':
        ml = 1000
        gph = int(token[1:4])
    elif token[0:1] == '2':
        ml = 925
        gph = int(token[1:4])
    elif token[0:1] == '5':
        ml = 500
        gph = int(token[1:4])
    elif token[0:1] == '7':
        ml = 700
        gph = int(token[1:4])
    elif token[0:1] == '8':
        ml = 850
        gph = int(token[1:4])
    return [ml, gph]

def getPPPP(token):
    if int(token[0:1]) <= 4:
        p = (10000 + int(token)) * 10
    else:
        p = int(token) * 10
    return [p]

def getsnTTT(token):
    if token[0:1] == '0':
        t = (int(token[1:4]) + 2732) / 10
    elif token[0:1] == '1':
        t = (int(token[1:4]) * -1 + 2732) / 10
    elif token[0:1] == '9':
        t = int(token[1:4])
    return [t]

def getNddff_00fff(token1, token2, wind_multiply):
    if token1[0:1] == '/':
        total_cloud = -1
    else:
        total_cloud = int(token1[0:1])
    if token1[1:3] == '//':
        wind_direction = -1
    else:
        wind_direction = int(token1[1:3]) * 10
    if token1[3:5] == '//':
        wind_speed = -1        
    else:
        wind_speed = int(token1[3:5])
    if len(token2) == 5 and re.match(r'^00[0-9]{3}$', token2):
        wind_speed = int(token2[2:5])
    elif wind_direction > 49 and wind_speed > -1:
        wind_speed = wind_speed + 100
    if wind_multiply > 0:
        wind_speed = round(wind_speed * wind_multiply * 10) / 10 
    else:
        wind_speed = -1
    return [total_cloud, wind_direction, wind_speed]

def getiRixhVV(token):
    is_precip = int(token[0:1])
    is_weather = int(token[1:2])
    if token[2:3] == '/':
        cloud_base = -1
    else:
        cloud_base = int(token[2:3])
    if token[3:5] == '//':
        visibility = -1
    else:
        visibility = int(token[3:5])
    return [is_precip, is_weather, cloud_base, visibility]

def get99LaLaLa_QcLoLoLoLo(token1, token2):
    lat = int(token1[2:5])
    lon = int(token2[1:5])
    if token2[0] == '3':
        lat = lat * -1
    elif token2[0] == '5':
        lat = lat * -1
        lon = lon * -1
    elif token2[0] == '7':
        lon = lon * -1
    if lon == -1800:
        lon = 1800
    lat = lat / 10
    lon = lon / 10
    return [lat, lon]

def getYYGGiw(token, dt_str, in_file):
    warno = 187
    if not dt_str[6:10] == token[0:4]:
        print('Warning', warno, ':', dt_str, 'in', in_file, 'does not match YYGG of', token, '.', file=sys.stderr)
        return []
    dt = datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), 0, 0, tzinfo=timezone.utc)
    wind_multiply = -1.0
    if token[4] == '0' or token[4] == '1':
        wind_multiply = 1.0
    elif token[4] == '3' or token[4] == '4':
        wind_multiply = 0.514444
    return [dt, wind_multiply]

def parse(cccc, cat, subcat, in_file, message, dt_str, debug):
    warno = 188
    an_dict = {}
    text = re.sub(' +', ' ', message.decode("ascii", errors="ignore").replace('\r', ' ').replace('\n', ' '))
    out_subcat = ''
    if cat == 'surface':
        if subcat == 'synop' or subcat == 'ship' or subcat == 'synop_mobil':
            if not re.search(r' (AAXX [0-9]{5}|BBXX|OOXX) ', text):
                print('Warning', warno, ':', in_file, 'does not match " (AAXX [0-9]{5}|BBXX|OOXX) ".', file=sys.stderr)
                return {}
            text = re.sub('\n$', '', re.sub(' *\n *', '\n', re.sub('( (AAXX [0-9]{5}|BBXX|OOXX) )', r'\n\1\n', text.replace('=', '\n'))))
            if debug:
                print('Debug', ':', text, file=sys.stderr)
            for line_num, line in enumerate(text.split('\n')):
                if line_num == 1:
                    if not re.search(r'^(AAXX [0-9]{5}|BBXX|OOXX)$', line):
                        print('Warning', warno, ':', 'The', line_num, 'line of', in_file, 'does not match "(AAXX [0-9]{5}|BBXX|OOXX)".', file=sys.stderr)
                        return {}
                    line_token_list = line.split(' ')
                    if line_token_list[0] == 'BBXX':
                        out_subcat = 'ship'
                    elif line_token_list[0] == 'OOXX':
                        out_subcat = 'synop_mobil'
                    else:
                        out_subcat = 'synop'
                        datetime_wind_multiply = getYYGGiw(line_token_list[1], dt_str, in_file)
                        if len(datetime_wind_multiply) != 2:
                            print('Warning', warno, ':', in_file, 'does not have valid datetime_wind_multiply.', file=sys.stderr)
                            return {}
                elif line_num > 1:
                    line_token_list = line.split(' ')
                    if out_subcat == 'ship':
                        if not re.search(r'^[0-9A-Z]+ [0-9]{5} 99([0-8][0-9]{2}|900) [1357](0[0-9]{3}|1[0-7][0-9]{2}|1800) [0-4][1-7][0-9/]([0-9]{2}|//) [0-9/]([0-2][0-9]|3[0-5]|//)([0-9]{2}|//) 1[0-9]{4}( 2[0-9]{4})*( 3[0-9]{4})* 4[0-9]{4} .*$', line):
                            print('Warning', warno, ':', 'The', line_num, 'line of', in_file, 'does not match.', file=sys.stderr)
                            continue
                        location = line_token_list[0]
                        datetime_wind_multiply = getYYGGiw(line_token_list[1], dt_str, in_file)
                        if len(datetime_wind_multiply) != 2:
                            print('Warning', warno, ':', in_file, 'does not have valid datetime_wind_multiply.', file=sys.stderr)
                            continue
                        lat_lon = get99LaLaLa_QcLoLoLoLo(line_token_list[2], line_token_list[3])
                        if len(lat_lon) != 2:
                            print('Warning', warno, ':', in_file, 'does not have valid lat_lon.', file=sys.stderr)
                            continue
                        rest_token_list = line_token_list[4:]
                        sc_num = -2
                        for token_num, token in enumerate(rest_token_list):
                            is_precip_is_weather_cloud_base_visibility = []
                            total_cloud_wind_direction_wind_speed = []
                            temperature = []
                            dewpoint_temperature = []
                            relative_humidity = []
                            pressure = []
                            pressure_reduced_to_msl = []
                            mandatory_level_geo_potential_height = []
                            if sc_num < -1 and token_num == 0:
                                is_precip_is_weather_cloud_base_visibility = getiRixhVV(token)
                                sc_num = -1
                            elif sc_num < 0 and token_num == 1:
                                total_cloud_wind_direction_wind_speed = getNddff_00fff(token, rest_token_list[token_num + 1], datetime_wind_multiply[1])
                                sc_num = 0
                            elif sc_num < 1 and re.match(r'^1[01][0-9]{3}$', token):
                                temperature = getsnTTT(token[1:])
                                sc_num = 1
                            elif sc_num < 2:
                                if re.match(r'^2[01][0-9]{3}$', token):
                                    dewpoint_temperature = getsnTTT(token[1:])
                                    sc_num = 2
                                elif re.match(r'^29[0-9]{3}$', token):
                                    relative_humidity = getsnTTT(token[1:])
                                    sc_num = 2
                            elif sc_num < 3 and re.match(r'^3[0-9]{4}$', token):
                                pressure = getPPPP(token[1:])
                                sc_num = 3
                            elif sc_num < 4:
                                if re.match(r'^4[09][0-9]{3}$', token):
                                    pressure_reduced_to_msl = getPPPP(token[1:])
                                    sc_num = 4
                                elif re.match(r'^4[12578][0-9]{3}$', token):
                                    mandatory_level_geo_potential_height = geta3hhh(token[1:])
                                    sc_num = 4
                            if len(location) > 0 and lat_lon[0] >= -90.0 and lat_lon[0] <= 90.0 and lat_lon[1] > -180.0 and lat_lon[1] <= 180.0 and len(pressure_reduced_to_msl) > 0:



                                if pressure_reduced_to_msl_name in an_dict:
                                    an_dict[pressure_reduced_to_msl_name] = np.concatenate([an_dict[pressure_reduced_to_msl_name], np.array(pressure_reduced_to_msl, dtype=object)])
                                else:
                                    an_dict[pressure_reduced_to_msl_name] = np.array(pressure_reduced_to_msl, dtype=object)
                                







                            

                        

                    


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
