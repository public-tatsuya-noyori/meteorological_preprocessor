#!/usr/bin/env python3
import argparse
import os
import re
import sys
import traceback
from pyarrow import csv

def convert_to_batch(in_list, seq_num, my_cccc, file_ext, out_dir, out_list, out_seq_num, debug):
    warno = 189
    out_batch_files = []
    batch_00_head_1 = bytes([48, 48, 1, 13, 13, 10]) # 0 0 SOH CR CR LF
    batch_00_head_2 = bytes([13, 13, 10]) # CR CR LF
    batch_00_foot = bytes([13, 13, 10, 3]) # CR CR LF ETX
    batch_01_head = bytes([48, 49, 13, 13, 10]) # 0 1 CR CR LF
    message_seq_num = seq_num.message
    file_seq_num = seq_num.file
    if seq_num.message_digit != 0 and file_seq_num == 0:
        message_seq_num = 0
    with open(in_list, 'r') as in_list_f:
        out_batch_file = ''
        out_batch_file_f = None
        is_new_batch_file = False
        for in_file in in_list_f.readlines():
            in_file = in_file.rstrip('\n')
            if out_batch_file:
                if seq_num.message_digit == 3:
                    if message_seq_num + 1 < 1000:
                        message_seq_num += 1
                    else:
                        message_seq_num = 1
                        is_new_batch_file = True
                elif seq_num.message_digit == 5:
                    if message_seq_num + 1 < 100000:
                        message_seq_num += 1
                    else:
                        message_seq_num = 1
                        is_new_batch_file = True
            if is_new_batch_file:
                if seq_num.file_digit == 8:
                    if file_seq_num + 1 < 100000000:
                        file_seq_num += 1
                    else:
                        file_seq_num = 1
                elif seq_num.file_digit == 4:
                    if file_seq_num + 1 < 10000:
                        file_seq_num += 1
                    else:
                        file_seq_num = 1
                out_batch_file_f.close()
                out_batch_files.append(out_batch_file)
                out_batch_file = ''
                is_new_batch_file = False
            if not out_batch_file:
                out_batch_file_list = []
                out_batch_file_list.append(out_dir)
                out_batch_file_list.append('/')
                out_batch_file_list.append(my_cccc)
                if seq_num.file_digit == 8:
                    out_batch_file_list.append(str(file_seq_num).zfill(8))
                elif seq_num.file_digit == 4:
                    out_batch_file_list.append(str(file_seq_num).zfill(4))
                out_batch_file_list.append('.')
                out_batch_file_list.append(file_ext)
                out_batch_file = ''.join(out_batch_file_list)
                out_batch_file_f = open(out_batch_file, 'wb')
            with open(in_file, 'rb') as in_file_f:
                message = in_file_f.read()
                message_list = bytearray()
                if seq_num.message_digit == 0:
                    message_length = len(message) + 3
                    if message_length > 99999999:
                        print('Warning', warno, ':', 'The message length of', in_file, 'is invalid. (>99999999)', file=sys.stderr)
                    else:
                        message_list.extend(str(message_length).zfill(8).encode())
                        message_list.extend(batch_01_head)
                        message_list.extend(message)
                        out_batch_file_f.write(message_list)
                else:
                    message_length = len(message) + 11 + seq_num.message_digit
                    if message_length > 99999999:
                        print('Warning', warno, ':', 'The message length of', in_file, 'is invalid. (>99999999)', file=sys.stderr)
                    else:
                        message_list.extend(str(message_length).zfill(8).encode())
                        message_list.extend(batch_00_head_1)
                        message_list.extend(str(message_seq_num).zfill(seq_num.message_digit).encode())
                        message_list.extend(batch_00_head_2)
                        message_list.extend(message)
                        message_list.extend(batch_00_foot)
                        out_batch_file_f.write(message_list)
                if debug:
                    print('Debug', ':', 'message_length =', message_length, 'seq_num.message_digit =', seq_num.message_digit, 'message_seq_num =', message_seq_num, 'seq_num.file_digit =', seq_num.file_digit, 'file_seq_num =', file_seq_num, file=sys.stderr)
        if out_batch_file:
            out_batch_file_f.close()
            out_batch_files.append(out_batch_file)
    with open(out_seq_num, 'w') as out_seq_num_f:
        out_seq_num_list = []
        out_seq_num_list.append('message_digit,message,file_digit,file\n')
        out_seq_num_list.append(str(seq_num.message_digit))
        out_seq_num_list.append(',')
        out_seq_num_list.append(str(message_seq_num + 1))
        out_seq_num_list.append(',')
        out_seq_num_list.append(str(seq_num.file_digit))
        out_seq_num_list.append(',')
        out_seq_num_list.append(str(file_seq_num + 1))
        out_seq_num_list.append('\n')
        out_seq_num_f.write(''.join(out_seq_num_list))
    if len(out_batch_files) > 0:
        print('\n'.join(out_batch_files), file=out_list)
        if debug:
            print('Debug', ':', len(out_batch_files), 'batch files have been saved.', file=sys.stderr)
    else:
        if debug:
            print('Debug', ':', 'No batch file has been saved.', file=sys.stderr)

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('input_list_file', metavar='input_list_file')
    parser.add_argument('input_sequential_number_csv_file', type=str, metavar='input_sequential_number_csv_file')
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('file_extension', type=str, metavar='file_extension', choices=['a','b','f','ua','ub'])
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument('output_sequential_number_csv_file', type=str, metavar='output_sequential_number_csv_file')
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if not os.access(args.input_list_file, os.F_OK):
        print('Error', errno, ':', args.input_list_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_sequential_number_csv_file, os.F_OK):
        print('Error', errno, ':', args.input_sequential_number_csv_file, 'does not exist.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.output_directory, os.F_OK):
        os.makedirs(args.output_directory, exist_ok=True)
    if not os.path.isfile(args.input_list_file):
        print('Error', errno, ':', args.input_list_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isfile(args.input_sequential_number_csv_file):
        print('Error', errno, ':', args.input_sequential_number_csv_file, 'is not file.', file=sys.stderr)
        sys.exit(errno)
    if not os.path.isdir(args.output_directory):
        print('Error', errno, ':', args.output_directory, 'is not directory.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_list_file, os.R_OK):
        print('Error', errno, ':', args.input_list_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not os.access(args.input_sequential_number_csv_file, os.R_OK):
        print('Error', errno, ':', args.input_sequential_number_csv_file, 'is not readable.', file=sys.stderr)
        sys.exit(errno)
    if not (os.access(args.output_directory, os.R_OK) and os.access(args.output_directory, os.W_OK) and os.access(args.output_directory, os.X_OK)):
        print('Error', errno, ':', args.output_directory, 'is not readable/writable/executable.', file=sys.stderr)
        sys.exit(errno)
    if not re.match(r'^[A-Z]{4}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z]{4}$).', file=sys.stderr)
        sys.exit(errno)
    try:
        seq_num = list(csv.read_csv(args.input_sequential_number_csv_file).to_pandas().itertuples())[0]
        if seq_num.file_digit == 8:
            if seq_num.file < 0 or seq_num.file > 99999999:
                print('Error', errno, ':', 'The sequential number of the file of', args.input_sequential_number_csv_file, 'is invalid (<0 or >99999999).', file=sys.stderr)
                sys.exit(errno)
        elif seq_num.file_digit == 4:
            if seq_num.file < 0 or seq_num.file > 9999:
                print('Error', errno, ':', 'The sequential number of the file of', args.input_sequential_number_csv_file, 'is invalid (<0 or >9999).', file=sys.stderr)
                sys.exit(errno)
        else:
            print('Error', errno, ':', 'The file digit of ', args.input_sequential_number_csv_file, 'is invalid (!=8 or !=4).', file=sys.stderr)
            sys.exit(errno)
        if seq_num.message_digit == 3:
            if seq_num.message < 1 or seq_num.message > 999:
               print('Error', errno, ':', 'The sequential number of message of', args.input_sequential_number_csv_file, 'is invalid (<1 or >999).', file=sys.stderr)
               sys.exit(errno)
        elif seq_num.message_digit == 5:
            if seq_num.message < 1 or seq_num.message > 99999:
               print('Error', errno, ':', 'The sequential number of message of', args.input_sequential_number_csv_file, 'is invalid (<1 or >99999).', file=sys.stderr)
               sys.exit(errno)
        elif seq_num.message_digit == 0:
            if seq_num.message != 0:
               print('Error', errno, ':', 'The sequential number of message of', args.input_sequential_number_csv_file, 'is invalid (!=0).', file=sys.stderr)
               sys.exit(errno)
        else:
            print('Error', errno, ':', 'The message digit of', args.input_sequential_number_csv_file, 'is invalid (!=3 or !=5 or !=0).', file=sys.stderr)
            sys.exit(errno)
        convert_to_batch(args.input_list_file, seq_num, args.my_cccc, args.file_extension, args.output_directory, args.output_list, args.output_sequential_number_csv_file, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
