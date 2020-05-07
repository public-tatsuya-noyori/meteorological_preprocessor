rm -rf cache/Open
met_pre_batch_to_cache --config ../config/batch_to_cache.csv batch cache --debug > cache_list.txt
met_pre_cache_to_batch cache_list.txt 0_seq_num.csv RJTD a batch out_0_seq_num.csv --debug
cp batch/RJTD00000001.a 0_RJTD00000001.a
met_pre_cache_to_batch cache_list.txt 3_seq_num.csv RJTD a batch out_3_seq_num.csv --debug
cp batch/RJTD99999999.a 3_RJTD99999999.a
cp batch/RJTD00000001.a 3_RJTD00000001.a
met_pre_cache_to_batch cache_list.txt 5_seq_num.csv RJTD a batch out_5_seq_num.csv --debug
cp batch/RJTD99999999.a 5_RJTD99999999.a
cp batch/RJTD00000001.a 5_RJTD00000001.a
