#!/usr/bin/env python

import numpy as np
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style='white')

df = pd.read_csv("join_measurements.csv")

fig, ax = plt.subplots(figsize = (12, 6))

df_agg = df.groupby(["PARTITION_COUNT", "PHASE"])["RUNTIME_MS"].mean()
df_pivoted = df_agg.reset_index().pivot(index="PARTITION_COUNT", columns="PHASE", values="RUNTIME_MS")

df_pivoted.plot(kind = "bar", stacked = True, ax = ax)
ax.set_xlabel("Partino Count")
ax.set_ylabel("Runtime (ms)")

plt.savefig("result_all.pdf")