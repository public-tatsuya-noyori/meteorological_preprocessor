#!/usr/bin/env python3
import os
import pyarrow as pa
import pandas as pd
import sys

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
df = pa.ipc.open_file(sys.argv[1]).read_pandas()
print(df.info())
print(df)
