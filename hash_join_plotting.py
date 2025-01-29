#!/usr/bin/env python

import numpy as np
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="white")


l2_cache_sizes = {"nx05": 1_310_720}
for id in range(1, 17):
    l2_cache_sizes[f"cx{id:02d}"] = 1_048_576
for id in range(17, 33):
    l2_cache_sizes[f"cx{id}"] = 524_288


def plot_machine(df, filename):
    assert "__" in filename
    assert "MACHINE" in df.columns
    machine = np.unique(df.MACHINE)[0]

    fig, ax = plt.subplots(figsize = (12, 6))

    df_agg = df.groupby(["PARTITION_COUNT", "PHASE"])["RUNTIME_MS"].mean()
    df_pivoted = df_agg.reset_index().pivot(index="PARTITION_COUNT", columns="PHASE", values="RUNTIME_MS")

    df_pivoted = df_pivoted[sorted(df_pivoted.columns, reverse=True)]

    df_pivoted.plot(kind = "bar", stacked = True, ax = ax)

    for c in ax.containers:
        labels = [f"{int(round(v.get_height(), 0)):,}" if v.get_height() > 0 else "" for v in c]
        ax.bar_label(c,
                     label_type="center",
                     labels = labels,
                     size = 12)

    ax.set_xlabel("Partition Count")
    ax.set_ylabel("Runtime (ms)")

    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=len(df.columns))

    plt.savefig(f"result__{machine}.pdf")


dataframes = []
for filename in os.listdir("."):
    if filename.startswith("join_measurements__") and filename.endswith(".csv"):
        df = pd.read_csv(filename)
        df["MACHINE"] = filename[filename.rfind("__")+2:filename.rfind(".")]
        dataframes.append(df)

        plot_machine(df, filename)
        plt.clf()

df = pd.concat(dataframes, ignore_index=True)

# df_agg1 = df.groupby(["MACHINE", "PARTITION_COUNT", "PHASE"])["RUNTIME_MS"].mean().reset_index()
# df_agg2 = df_agg1.groupby(["MACHINE", "PARTITION_COUNT"])["RUNTIME_MS"].sum().reset_index()
sns.relplot(df, x="PARTITION_COUNT", y="RUNTIME_MS", hue="MACHINE", col="PHASE", kind="line", errorbar="sd", facet_kws=dict(sharey=False))
plt.xscale("log")

plt.savefig("all_machines.pdf")