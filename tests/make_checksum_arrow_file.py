#!/usr/bin/env python3
import pyarrow as pa
from pyarrow import feather

with open('checksum.feather', 'bw') as out_f:
    property_batch = pa.record_batch([[], []], names=['mtime', 'checksum'])
    property_table = pa.Table.from_batches([property_batch])
    feather.write_feather(property_table, out_f, compression='zstd')
