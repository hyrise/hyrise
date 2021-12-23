#! /usr/bin/env python3

import sys
import pandas as pd

assert len(sys.argv) == 3, "Usage: {} old.csv new.csv".format(sys.argv[0])

df_old = pd.read_csv(sys.argv[1], index_col=None)
df_new = pd.read_csv(sys.argv[2], index_col=None)

print(df_old)
print(df_new)

df = df_new
numeric_columns = ["Parser", "SQLTranslator", "Optimizer", "LQPTranslator", "Execution"]
df[numeric_columns] = (df[numeric_columns] / df_old[numeric_columns] - 1) * 100
print(df)
exit(0)
