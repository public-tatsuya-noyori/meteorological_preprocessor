rm -rf cache/open
met_pre_batch_file_to_wmo_codes_cache batch cache --output_list_file cache_list.txt --debug
met_pre_wmo_codes_cache_to_batch_file cache_list.txt in_0_seq_num.csv RJTD a batch out_0_seq_num.csv --debug
cp batch/RJTD00000000.a 0_RJTD00000000.a
met_pre_wmo_codes_cache_to_batch_file cache_list.txt in_3_seq_num.csv RJTD a batch out_3_seq_num.csv --debug
cp batch/RJTD99999999.a 3_RJTD99999999.a
cp batch/RJTD00000001.a 3_RJTD00000001.a
met_pre_wmo_codes_cache_to_batch_file cache_list.txt in_5_seq_num.csv RJTD a batch out_5_seq_num.csv --debug
cp batch/RJTD99999999.a 5_RJTD99999999.a
cp batch/RJTD00000001.a 5_RJTD00000001.a
