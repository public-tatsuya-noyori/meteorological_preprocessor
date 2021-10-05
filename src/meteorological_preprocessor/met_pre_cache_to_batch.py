#!/usr/bin/env python3
import argparse
import os
import re
import sys
import traceback
from pyarrow import csv

def convert_to_batch(my_cccc, in_file_list, seq_num, file_ext, out_dir, out_list_file, out_seq_num_file, limit_num, limit_size, debug):
    warno = 189
    out_batch_file_counter = 0
    batch_00_head_1 = bytes([48, 48, 1, 13, 13, 10]) # 0 0 SOH CR CR LF
    batch_00_head_2 = bytes([13, 13, 10]) # CR CR LF
    batch_00_foot = bytes([13, 13, 10, 3]) # CR CR LF ETX
    batch_01_head = bytes([48, 49, 13, 13, 10]) # 0 1 CR CR LF
    message_seq_num = seq_num.message
    file_seq_num = seq_num.file
    if seq_num.message_digit != 0 and file_seq_num == 0:
        message_seq_num = 0
    out_batch_file = ''
    out_batch_file_stream = None
    is_new_batch_file = False
    message_counter = 0
    for in_file in in_file_list:
        try:
            if os.path.getsize(in_file) > limit_size:
                print('Warning', warno, ':', 'The size of', in_file, 'is over the limitation of the size of a file. The file is not added to a batch file.', file=sys.stderr)
                continue
        except:
            print('Warning', warno, ':', 'can not read the size of', in_file, '. The file is not added to a batch file.', file=sys.stderr)
            continue
        if out_batch_file:
            if message_counter == limit_num:
                is_new_batch_file = True
                message_counter = 0
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
            out_batch_file_stream.close()
            print(out_batch_file, file=out_list_file)
            out_batch_file_counter += 1
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
            out_batch_file_stream = open(out_batch_file, 'wb')
        with open(in_file, 'rb') as in_file_stream:
            message = in_file_stream.read()
            message_batch = bytearray()
            if seq_num.message_digit == 0:
                message_length = len(message) + 3
                if message_length > 99999999:
                    print('Warning', warno, ':', 'The message length of', in_file, 'is invalid. (>99999999)', file=sys.stderr)
                else:
                    message_batch.extend(str(message_length).zfill(8).encode())
                    message_batch.extend(batch_01_head)
                    message_batch.extend(message)
                    out_batch_file_stream.write(message_batch)
                    message_counter += 1
            else:
                message_length = len(message) + 11 + seq_num.message_digit
                if message_length > 99999999:
                    print('Warning', warno, ':', 'The message length of', in_file, 'is invalid. (>99999999)', file=sys.stderr)
                else:
                    message_batch.extend(str(message_length).zfill(8).encode())
                    message_batch.extend(batch_00_head_1)
                    message_batch.extend(str(message_seq_num).zfill(seq_num.message_digit).encode())
                    message_batch.extend(batch_00_head_2)
                    message_batch.extend(message)
                    message_batch.extend(batch_00_foot)
                    out_batch_file_stream.write(message_batch)
                    message_counter += 1
            if debug:
                print('Debug', ':', 'message_length =', message_length, 'seq_num.message_digit =', seq_num.message_digit, 'message_seq_num =', message_seq_num, 'seq_num.file_digit =', seq_num.file_digit, 'file_seq_num =', file_seq_num, file=sys.stderr)
    if out_batch_file:
        out_batch_file_stream.close()
        print(out_batch_file, file=out_list_file)
        out_batch_file_counter += 1
    with open(out_seq_num_file, 'w') as out_seq_num_file_stream:
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
        out_seq_num_file_stream.write(''.join(out_seq_num_list))

def main():
    errno=198
    parser = argparse.ArgumentParser()
    parser.add_argument('my_cccc', type=str, metavar='my_cccc')
    parser.add_argument('input_list_file', metavar='input_list_file')
    parser.add_argument('input_sequential_number_csv_file', type=str, metavar='input_sequential_number_csv_file')
    parser.add_argument('file_extension', type=str, metavar='file_extension', choices=['a','b','f','ua','ub'])
    parser.add_argument('output_directory', type=str, metavar='output_directory')
    parser.add_argument('--output_list_file', type=argparse.FileType('w'), metavar='output_list_file', default=sys.stdout)
    parser.add_argument('output_sequential_number_csv_file', type=str, metavar='output_sequential_number_csv_file')
    parser.add_argument("--limit_num", type=int, metavar='limitation of the number of messages in a file. default = 100)', default=100)
    parser.add_argument("--limit_size", type=int, metavar='limitation of the size of a message. default = 1048576 = 1MB', default=1048576)
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    if not re.match(r'^[A-Z][A-Z0-9]{3}$', args.my_cccc):
        print('Error', errno, ':', 'CCCC of', args.my_cccc, 'is invalid (!=^[A-Z][A-Z0-9]{3}$).', file=sys.stderr)
        sys.exit(errno)
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
        input_file_list = []
        with open(args.input_list_file, 'r') as in_list_file_stream:
            input_file_list = [in_file.rstrip('\n') for in_file in in_list_file_stream.readlines()]
        convert_to_batch(args.my_cccc, input_file_list, seq_num, args.file_extension, args.output_directory, args.output_list_file, args.output_sequential_number_csv_file, args.limit_num, args.limit_size, args.debug)
    except:
        traceback.print_exc(file=sys.stderr)
        sys.exit(199)

if __name__ == '__main__':
    main()
