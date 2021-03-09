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

constant_pressure_level_name = 'constant pressure level [Pa]'
datetime_name = 'datetime'
geopotential_height = 'geopotential height [m]'
height_of_base_of_lowest_cloud_name = 'height of base of lowest cloud'
height_of_station_ground_above_mean_sea_level_name = 'height of station ground above mean sea level [m]'
horizontal_visibility_name = 'horizontal visibility'
is_precip_name = 'is_precip'
is_weather_name = 'is_weather'
latitude_name = 'latitude [degree]'
longitude_name = 'longitude [degree]'
location_name = 'location'
dewpoint_temperature_name = 'dewpoint temperature [K]'
pressure_name = 'pressure [Pa]'
pressure_reduced_to_mean_sea_level_name = 'pressure reduced to mean sea level [Pa]'
relative_humidity_name = 'relative humidity [%]'
temperature_name = 'temperature [K]'
total_cloud_name = 'total cloud'
wind_speed_name = 'wind speed [m/s]'
wind_direction_name = 'wind direction [degree]'
wind_multiply_name = 'wind_multiply'

def geth0h0h0h0im(token, elem_dict):
    if token[4:5] == '1' or token[4:5] == '2':
        elem_dict[height_of_station_ground_above_mean_sea_level_name] = int(token[0:4])
    elif token[4:5] == '5' or token[4:5] == '6':
        elem_dict[height_of_station_ground_above_mean_sea_level_name] = int(token[0:4]) * 3.28084
    return elem_dict

def geta3hhh(token, elem_dict):
    if token[0:1] == '1':
        elem_dict[constant_pressure_level_name] = 10000
        elem_dict[geopotential_height] = int(token[1:4])
    elif token[0:1] == '2':
        elem_dict[constant_pressure_level_name] = 9250
        elem_dict[geopotential_height] = int(token[1:4])
    elif token[0:1] == '5':
        elem_dict[constant_pressure_level_name] = 5000
        elem_dict[geopotential_height] = int(token[1:4])
    elif token[0:1] == '7':
        elem_dict[constant_pressure_level_name] = 7000
        elem_dict[geopotential_height] = int(token[1:4])
    elif token[0:1] == '8':
        elem_dict[constant_pressure_level_name] = 8500
        elem_dict[geopotential_height] = int(token[1:4])
    return elem_dict

def getPPPP(token, elem_dict, elem_name):
    if int(token[0:1]) <= 4:
        elem_dict[elem_name] = (10000 + int(token))
    else:
        elem_dict[elem_name] =  int(token)
    return elem_dict

def getsnTTT(token, elem_dict, elem_name):
    if token[0:1] == '0':
        elem_dict[elem_name] = (int(token[1:4]) + 2732) / 10
    elif token[0:1] == '1':
        elem_dict[elem_name] = (int(token[1:4]) * -1 + 2732) / 10
    elif token[0:1] == '9':
        elem_dict[relative_humidity_name] = int(token[1:4])
    return elem_dict

def getNddff_00fff(token1, token2, wind_multiply, elem_dict):
    if token1[0:1] != '/':
        elem_dict[total_cloud_name] = int(token1[0:1])
    if token1[1:3] == '//':
        wind_direction = -1
    elif token1[1:3] == '36':
        wind_direction = 0
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
    if wind_direction >= 0:
        if wind_speed == 0:
            wind_direction = 0
        elif wind_speed < 0:
            wind_direction = -1
    if wind_multiply > 0 and wind_speed >= 0:
        elem_dict[wind_speed_name] = round(wind_speed * wind_multiply * 10) / 10
        if wind_speed == 0:
            elem_dict[wind_direction_name] = 0
        elif wind_direction > 0:
            elem_dict[wind_direction_name] = wind_direction
    return elem_dict

def getiRixhVV(token, elem_dict):
    elem_dict[is_precip_name] = int(token[0:1])
    elem_dict[is_weather_name] = int(token[1:2])
    if token[2:3] != '/':
        elem_dict[height_of_base_of_lowest_cloud_name] = int(token[2:3])
    if token[3:5] != '//':
        elem_dict[horizontal_visibility_name] = int(token[3:5])
    return elem_dict

def get99LaLaLa_QcLoLoLoLo(token1, token2, elem_dict):
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
    elem_dict[latitude_name] = lat / 10
    elem_dict[longitude_name] = lon / 10
    return elem_dict

def getYYGGiw(token, dt_str, in_file, elem_dict):
    warno = 187
    if not dt_str[6:10] == token[0:4]:
        print('Warning', warno, ':', dt_str, 'in', in_file, 'does not match YYGG of', token, '.', file=sys.stderr)
        return elem_dict
    elem_dict[datetime_name] = datetime(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8]), int(dt_str[8:10]), int(dt_str[10:12]), 0, 0, tzinfo=timezone.utc)
    if token[4] == '0' or token[4] == '1':
        elem_dict[wind_multiply_name] = 1.0
    elif token[4] == '3' or token[4] == '4':
        elem_dict[wind_multiply_name] = 0.514444
    else:
        elem_dict[wind_multiply_name] = -1.0
    return elem_dict

def parse(cccc, cat, subcat, output_cat, output_subcat, in_file, message, dt_str, conf_synop_staion_df, conf_temp_pilot_staion_df, debug):
    warno = 188
    an_dict = {}
    datatype_dict = {}
    text = re.sub(' +', ' ', message.decode("ascii", errors="ignore").replace('\r', ' ').replace('\n', ' '))
    out_subcat = ''
    if cat == 'surface':
        if subcat == 'synop' or subcat == 'ship' or subcat == 'synop_mobil':
            if not re.search(r' (AAXX [0-9]{4}[0-9/]|BBXX|OOXX) ', text):
                if not re.search(r'^NIL$', text):
                    print('Warning', warno, ':', in_file, 'does not match " (AAXX [0-9]{4}[0-9/]|BBXX|OOXX) ".', file=sys.stderr)
                return {}, {}
            text = re.sub('\n$', '', re.sub(' *\n *', '\n', re.sub('( (AAXX [0-9]{4}[0-9/]|BBXX|OOXX) )', r'\n\1\n', text.replace('=', '\n'))))
            if debug:
                print('Debug', ':', text, file=sys.stderr)
            elem_dict = {}
            initialized_elem_dict = {}
            for line_num, line in enumerate(text.split('\n')):
                if line_num == 1:
                    if not re.search(r'^(AAXX [0-9]{4}[0-9/]|BBXX|OOXX)$', line):
                        print('Warning', warno, ':', 'The', line_num, 'line of', in_file, 'does not match "(AAXX [0-9]{4}[0-9/]|BBXX|OOXX)".', file=sys.stderr)
                        return {}, {}
                    line_token_list = line.split(' ')
                    if subcat == 'synop' and line_token_list[0] == 'AAXX':
                        initialized_elem_dict = getYYGGiw(line_token_list[1], dt_str, in_file, initialized_elem_dict)
                        if not datetime_name in initialized_elem_dict:
                            print('Warning', warno, ':', in_file, 'does not have valid datetime.', file=sys.stderr)
                            continue
                elif line_num > 1:
                    line_token_list = line.split(' ')
                    elem_dict = initialized_elem_dict
                    if cat == 'surface':
                        if subcat == 'ship' or subcat == 'synop' or subcat == 'synop_mobil':
                            rest_token_list = []
                            if subcat == 'ship':
                                if not re.search(r'^[0-9A-Z]+ [0-9]{5} 99([0-8][0-9]{2}|900) [1357](0[0-9]{3}|1[0-7][0-9]{2}|1800) [0-4][1-7][0-9/]([0-9]{2}|//) [0-9/]([0-2][0-9]|3[0-6]|//)([0-9]{2}|//) 1[0-9/]{4}( 2[0-9/]{4})*( 3[0-9/]{4})* 4[0-9/]{4}.*$', line):
                                    if not re.search(r'^NIL$', line) and not re.search(r'^[0-9A-Z]+ NIL$', line):
                                        print('Warning', warno, ':', line, 'of', in_file, 'does not match.', file=sys.stderr)
                                    continue
                                elem_dict[location_name] = line_token_list[0]
                                elem_dict = getYYGGiw(line_token_list[1], dt_str, in_file, elem_dict)
                                if not datetime_name in elem_dict:
                                    print('Warning', warno, ':', in_file, 'does not have valid datetime.', file=sys.stderr)
                                    continue
                                elem_dict = get99LaLaLa_QcLoLoLoLo(line_token_list[2], line_token_list[3], elem_dict)
                                rest_token_list = line_token_list[4:]
                                sc_num = -2
                            elif subcat == 'synop_mobil':
                                if not re.search(r'^[0-9A-Z]+ [0-9]{5} 99([0-8][0-9]{2}|900) [1357](0[0-9]{3}|1[0-7][0-9]{2}|1800) [0-5][0-9]{2}[0-9]{2} [0-8][0-9]{3}[1256] [0-4][1-7][0-9/]([0-9]{2}|//) [0-9/]([0-2][0-9]|3[0-6]|//)([0-9]{2}|//) 1[0-9/]{4}( 2[0-9/]{4})*( 3[0-9/]{4})*( 4[0-9/]{4})*.*$', line):
                                    if not re.search(r'^[0-9A-Z]+ [0-9]{5} 99([0-8][0-9]{2}|900) [1357](0[0-9]{3}|1[0-7][0-9]{2}|1800) [0-5][0-9]{2}[0-9]{2} ////[1256] [0-4][1-7][0-9/]([0-9]{2}|//) [0-9/]([0-2][0-9]|3[0-6]|//)([0-9]{2}|//) 1[0-9/]{4}( 2[0-9/]{4})*( 3[0-9/]{4})*( 4[0-9/]{4})*.*$', line):
                                        print('Warning', warno, ':', line, 'of', in_file, 'does not match.', file=sys.stderr)
                                    continue
                                elem_dict[location_name] = line_token_list[0]
                                elem_dict = getYYGGiw(line_token_list[1], dt_str, in_file, elem_dict)
                                if not datetime_name in elem_dict:
                                    print('Warning', warno, ':', in_file, 'does not have valid datetime.', file=sys.stderr)
                                    continue
                                elem_dict = get99LaLaLa_QcLoLoLoLo(line_token_list[2], line_token_list[3], elem_dict)
                                elem_dict = geth0h0h0h0im(line_token_list[5], elem_dict)
                                rest_token_list = line_token_list[6:]
                                sc_num = -2
                            elif subcat == 'synop':
                                if re.search(r'^AAXX [0-9]{4}[0-9/]$', line):
                                    line_token_list = line.split(' ')
                                    initialized_elem_dict = getYYGGiw(line_token_list[1], dt_str, in_file, initialized_elem_dict)
                                    if not datetime_name in initialized_elem_dict:
                                        print('Warning', warno, ':', in_file, 'does not have valid datetime.', file=sys.stderr)
                                        continue
                                if not re.search(r'^[0-9]{5} [0-4][1-7][0-9/]([0-9]{2}|//) [0-9/]([0-2][0-9]|3[0-6]|//)([0-9]{2}|//) 1[0-9]{4}( 2[0-9/]{4})*( 3[0-9/]{4})* 4[0-9/]{4}.*$', line):
                                    if not re.search(r'^NIL$', line) and not re.search(r'^[0-9]{5} NIL$', line):
                                        print('Warning', warno, ':', line, 'of', in_file, 'does not match.', file=sys.stderr)
                                    continue
                                synop_station = conf_synop_staion_df[conf_synop_staion_df[location_name] == re.sub(r'^0+', '', line_token_list[0])]
                                if len(synop_station) != 1:
                                    print('Info', ':', 'conf_synop_staion.csv does not have the location of', line_token_list[0], file=sys.stderr)
                                    continue
                                elem_dict[location_name] = int(synop_station[location_name])
                                elem_dict[latitude_name] = float(synop_station[latitude_name])
                                elem_dict[longitude_name] = float(synop_station[longitude_name])
                                elem_dict[height_of_station_ground_above_mean_sea_level_name] = int(synop_station[height_of_station_ground_above_mean_sea_level_name])
                                rest_token_list = line_token_list[1:]
                                sc_num = -2
                            for token_num, token in enumerate(rest_token_list):
                                if sc_num < -1 and re.match(r'^[0-4][1-7][0-9/]([0-9]{2}|//)', token):
                                    elem_dict = getiRixhVV(token, elem_dict)
                                    sc_num = -1
                                elif sc_num < 0 and re.match(r'^[0-9/]([0-2][0-9]|3[0-5]|//)([0-9]{2}|//)', token):
                                    elem_dict = getNddff_00fff(token, rest_token_list[token_num + 1], elem_dict[wind_multiply_name], elem_dict)
                                    sc_num = 0
                                elif sc_num < 1 and re.match(r'^1[01][0-9]{3}$', token):
                                    elem_dict = getsnTTT(token[1:], elem_dict, temperature_name)
                                    sc_num = 1
                                elif sc_num < 2:
                                    if re.match(r'^2[01][0-9]{3}$', token):
                                        elem_dict = getsnTTT(token[1:], elem_dict, dewpoint_temperature_name)
                                        sc_num = 2
                                    elif re.match(r'^29[0-9]{3}$', token):
                                        elem_dict = getsnTTT(token[1:], elem_dict, relative_humidity_name)
                                        sc_num = 2
                                elif sc_num < 3 and re.match(r'^3[0-9]{4}$', token):
                                    elem_dict = getPPPP(token[1:], elem_dict, pressure_name)
                                    sc_num = 3
                                elif sc_num < 4:
                                    if re.match(r'^4[09][0-9]{3}$', token):
                                        elem_dict = getPPPP(token[1:], elem_dict, pressure_reduced_to_mean_sea_level_name)
                                        sc_num = 4
                                    elif re.match(r'^4[12578][0-9]{3}$', token):
                                        elem_dict = geta3hhh(token[1:], elem_dict)
                                        sc_num = 4
                                    elif re.match(r'^4[0125789][/]{3}$', token):
                                        sc_num = 4
                            if datetime_name in elem_dict and location_name in elem_dict and latitude_name in elem_dict and longitude_name in elem_dict and len(elem_dict) > 3:
                                data_list = [datetime_name, location_name, latitude_name, longitude_name, pressure_reduced_to_mean_sea_level_name, pressure_name, temperature_name, dewpoint_temperature_name, relative_humidity_name, wind_speed_name, wind_direction_name]
                                if subcat == 'synop' or subcat == 'synop_mobil':
                                    data_list.append(height_of_station_ground_above_mean_sea_level_name)
                                for key in data_list:
                                    if key in [latitude_name, longitude_name, height_of_station_ground_above_mean_sea_level_name, pressure_reduced_to_mean_sea_level_name, pressure_name, temperature_name, dewpoint_temperature_name, relative_humidity_name, wind_speed_name, wind_direction_name]:
                                        datatype_dict[key] = 'float64'
                                    elif key in [height_of_base_of_lowest_cloud_name]:
                                        datatype_dict[key] = 'int32'
                                    elif key == location_name:
                                        if subcat == 'ship' or subcat == 'synop_mobil':
                                            datatype_dict[location_name] = 'string'
                                        elif subcat == 'synop':
                                            datatype_dict[location_name] = 'int32'
                                    value = None
                                    if key in elem_dict:
                                        value = elem_dict[key]
                                    if key in an_dict:
                                        an_dict[key] = np.concatenate([an_dict[key], np.array([value], dtype=object)])
                                    else:
                                        an_dict[key] = np.array([value], dtype=object)
                        
                    #elif output_cat == 'upper_air':
    return an_dict, datatype_dict

def convert_to_arrow(my_cccc, in_file_list, out_dir, out_list_file, conf_df, conf_synop_staion_df, conf_temp_pilot_staion_df, debug):
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
            out_cat_subcat_df = conf_df[(conf_df['input_category'] == cat) & (conf_df['input_subcategory'] == subcat)]
            location_type_output_cat_subcat_set = set([str(location_type) + '/' + output_cat + '/' + output_subcat for output_index, location_type, output_cat, output_subcat in list(out_cat_subcat_df[['location_type','output_category','output_subcategory']].itertuples())])
            for location_type_output_cat_subcat in location_type_output_cat_subcat_set:
                property_dict = {}
                datatype_dict = {}
                location_type_output_cat_subcat_list = location_type_output_cat_subcat.split('/')
                location_type = int(location_type_output_cat_subcat_list[0])
                output_cat = location_type_output_cat_subcat_list[1]
                output_subcat = location_type_output_cat_subcat_list[2]
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
                    an_dict, datatype_dict = parse(cccc, cat, subcat, output_cat, output_subcat, in_file, message, dt_str, conf_synop_staion_df, conf_temp_pilot_staion_df, debug)
                    for key in an_dict:
                        message_np = an_dict[key]
                        if key in property_dict:
                            property_dict[key] = np.concatenate([property_dict[key], message_np])
                        else:
                            property_dict[key] = message_np
                if datetime_name in property_dict and location_name in property_dict and location_name in datatype_dict:
                    id_list = [id_num for id_num in range(0, len(property_dict[datetime_name]))]
                    location_datetime_name_list = ['id']
                    location_datetime_data = [pa.array(id_list, 'int32')]
                    location_datetime_name_list.append(location_name)
                    location_datetime_data.append(pa.array(property_dict[location_name], datatype_dict[location_name]))
                    property_dict.pop(location_name)
                    datatype_dict.pop(location_name)
                    location_datetime_name_list.append(latitude_name)
                    location_datetime_data.append(pa.array(property_dict[latitude_name], datatype_dict[latitude_name]))
                    property_dict.pop(latitude_name)
                    datatype_dict.pop(latitude_name)
                    location_datetime_name_list.append(longitude_name)
                    location_datetime_data.append(pa.array(property_dict[longitude_name], datatype_dict[longitude_name]))
                    property_dict.pop(longitude_name)
                    datatype_dict.pop(longitude_name)
                    if subcat == 'synop' or subcat == 'synop_mobil':
                        location_datetime_name_list.append(height_of_station_ground_above_mean_sea_level_name)
                        location_datetime_data.append(pa.array(property_dict[height_of_station_ground_above_mean_sea_level_name], datatype_dict[height_of_station_ground_above_mean_sea_level_name]))
                        property_dict.pop(height_of_station_ground_above_mean_sea_level_name)
                        datatype_dict.pop(height_of_station_ground_above_mean_sea_level_name)
                    location_datetime_name_list.append(datetime_name)
                    location_datetime_data.append(pa.array(property_dict[datetime_name], pa.timestamp('ms', tz='utc')))
                    datetime_directory_list = []
                    for dt in set(property_dict[datetime_name]):
                        dt_str = dt.strftime('%Y%m%d%H%M')
                        if not dt_str[0:11] + "0" in datetime_directory_list:
                            datetime_directory_list.append(dt_str[0:11] + "0")
                    datetime_len = 11
                    for datetime_directory in datetime_directory_list:
                        datetime_index_list = [index for index, value in enumerate(property_dict['datetime']) if value.strftime('%Y%m%d%H%M')[0:datetime_len] == datetime_directory[0:datetime_len]]
                        if len(datetime_index_list) > 0:
                            tmp_location_datetime_data = [location_datetime.take(pa.array(datetime_index_list)) for location_datetime in location_datetime_data]
                            if len(tmp_location_datetime_data) > 0:
                                out_directory_list = [out_dir, cccc, 'alphanumeric_to_arrow', output_cat, output_subcat, datetime_directory, create_datetime_directory]
                                out_directory = '/'.join(out_directory_list)
                                os.makedirs(out_directory, exist_ok=True)
                                out_file_list = [out_directory, 'location_datetime.feather']
                                out_file = '/'.join(out_file_list)
                                with open(out_file, 'bw') as out_f:
                                    location_datetime_batch = pa.record_batch(tmp_location_datetime_data, names=location_datetime_name_list)
                                    location_datetime_table = pa.Table.from_batches([location_datetime_batch])
                                    feather.write_feather(location_datetime_table, out_f, compression='zstd')
                                    print(out_file, file=out_list_file)
                                property_key_list = [property_key for property_key in property_dict.keys() if property_key != datetime_name]
                                for property_key in property_key_list:
                                    property_name_list = ['id']
                                    property_name_list.append(property_key)
                                    property_data = []
                                    datetime_id_pa = pa.array(id_list, 'int32').take(pa.array(datetime_index_list))
                                    if max(datetime_index_list) < len(property_dict[property_key]):
                                        datetime_property_data = pa.array(property_dict[property_key][datetime_index_list].tolist(), datatype_dict[property_key])
                                        value_index_list = [index for index, value in enumerate(datetime_property_data.tolist()) if value != None]
                                        if len(value_index_list) > 0:
                                            property_data.append(datetime_id_pa.take(pa.array(value_index_list)))
                                            property_data.append(datetime_property_data.take(pa.array(value_index_list)))
                                            out_directory_list = [out_dir, cccc, 'alphanumeric_to_arrow', output_cat, output_subcat, datetime_directory, create_datetime_directory]
                                            out_directory = '/'.join(out_directory_list)
                                            os.makedirs(out_directory, exist_ok=True)
                                            out_file_list = [out_directory, '/', re.sub(' ', '_', re.sub(' \[.*$', '', property_key)), '.feather']
                                            out_file = ''.join(out_file_list)
                                            with open(out_file, 'bw') as out_f:
                                                property_batch = pa.record_batch(property_data, names=property_name_list)
                                                property_table = pa.Table.from_batches([property_batch])
                                                feather.write_feather(property_table, out_f, compression='zstd')
                                                print(out_file, file=out_list_file)
                                    else:
                                        print('Info', output_cat, output_subcat, 'max(datetime_index_list) >= len(property_dict[property_key]) key :', property_key, max(datetime_index_list), len(property_dict[property_key]), file=sys.stderr)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', type=str, metavar='input_list_file')
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument("--config_synop_staion", type=str, metavar='conf_synop_staion.csv', default=pkg_resources.resource_filename(__name__, 'conf_synop_staion.csv'))
    parser.add_argument("--config_temp_pilot_staion", type=str, metavar='conf_temp_pilot_staion.csv', default=pkg_resources.resource_filename(__name__, 'conf_temp_pilot_staion.csv'))
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    config = pkg_resources.resource_filename(__name__, 'conf_alphanumeric_to_arrow.csv')
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
    if not os.access(args.config_synop_staion, os.F_OK):
        print('Error', errno, ':', args.config_synop_staion, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_temp_pilot_staion, os.F_OK):
        print('Error', errno, ':', args.config_temp_pilot_staion, 'does not exist.', file=sys.stderr)
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
    if not os.path.isfile(args.config_synop_staion):
        print('Error', errno, ':', args.config_synop_staion, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.config_temp_pilot_staion):
        print('Error', errno, ':', args.config_temp_pilot_staion, 'is not file.', file=sys.stderr)
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
    if not os.access(args.config_synop_staion, os.R_OK):
        print('Error', errno, ':', args.config_synop_staion, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.config_temp_pilot_staion, os.R_OK):
        print('Error', errno, ':', args.config_temp_pilot_staion, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    try:
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        conf_df = csv.read_csv(config).to_pandas()
        conf_synop_staion_df = csv.read_csv(args.config_synop_staion).to_pandas()
        conf_temp_pilot_staion_df = csv.read_csv(args.config_temp_pilot_staion).to_pandas()
        convert_to_arrow(args.my_cccc, input_file_list, args.output_directory, args.output_list_file, conf_df, conf_synop_staion_df, conf_temp_pilot_staion_df, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
