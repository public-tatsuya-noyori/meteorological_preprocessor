#!/usr/bin/env python3
import os
import pyarrow as pa
import pandas as pd
import sys

if sys.argv[1] == "-v":
    df = pa.ipc.open_file(sys.argv[2]).read_pandas()
    print(df.values)
else:
    df = pa.ipc.open_file(sys.argv[1]).read_pandas()
    print(df.dtypes)
    print(df)
