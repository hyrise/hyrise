#!/usr/bin/env python3

import os
import re
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import seaborn as sns

def load_csv(path):
    if os.path.isdir(path):
        print(f"The provided path '{path}' is a directory. Should be csv file.")
        return
    if not path.endswith('.csv'):
        print(f"The provided path '{path}' is not a csv file.")
        return
    df = pd.read_csv(path, names=["table_name", "column_name", "accesses", "size_GB"], header=None)
    return df

if __name__ == "__main__":
    is_print = False

    csv_path = None
    if len(sys.argv) >= 2:
        csv_path = sys.argv[1]
    else:
        print("Usage: python script.py <csv_path> [print]")
        sys.exit(1)
    if len(sys.argv) == 3:
        if sys.argv[2] == "print":
            is_print = True

    df = load_csv(csv_path)
    df["accesses_G"] = df["accesses"] / 1000000000
    if is_print:
      print(df[1:16])

    print(df[["size_GB","accesses_G"]].sum())

    plt.rcParams.update({
        'text.usetex': True,
        'font.family': 'serif',
        'text.latex.preamble': r'\usepackage{libertine}'
    })

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 1.6), sharex=True, gridspec_kw={'hspace': 0.1})

    # Plot Accesses
    ax1 = sns.barplot(data=df, x="column_name", y="accesses_G", ax=axes[0])
    # ax1.set_yscale("log")
    axes[0].set_ylabel("Accesses\n[Billion]")
    ax1.grid(axis='y', linestyle=':', alpha=0.7)
    # ax1.grid(axis='x', linestyle='-', alpha=0.7)
    ax1.yaxis.set_major_locator(ticker.MultipleLocator(2))
    ax1.yaxis.set_minor_locator(ticker.MultipleLocator(1))
    ax1.set_xlim(-0.5, len(df) - 0.5)

    # Plot Size
    ax2 = sns.barplot(data=df, x="column_name", y="size_GB", ax=axes[1])
    # ax2.set_yscale("log")
    axes[1].set_xticklabels(df["column_name"], rotation=90)
    axes[1].set_xlabel("Column [Name]")
    axes[1].set_ylabel("Size\n[GB]")
    ax2.grid(axis='y', linestyle=':', alpha=0.7)
    # ax2.grid(axis='x', linestyle='-', alpha=0.7)
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(5))
    ax2.yaxis.set_minor_locator(ticker.MultipleLocator(2.5))

    # Cumulative sum
    df["size_cumulative_sum"] = df["size_GB"].cumsum()
    df["accesses_cumulative_sum"] = df["accesses_G"].cumsum()
    df_subset = df.iloc[:20]
    print(df_subset)
    print(df_subset["column_name"].unique())
    # ax1.plot(df_subset["column_name"], df_subset["accesses_cumulative_sum"], color="C3", marker="o", markersize=3, linestyle="-", label="Cumulative Sum")
    # ax1.legend()
    ax2.plot(df_subset["column_name"], df_subset["size_cumulative_sum"], color="C3", marker="o", markersize=3, linestyle="-", label="Cumulative Sum")
    ax2.legend()

    plt.tight_layout()

    plot_path = f"{csv_path.rsplit('/',1)[0]}/plot_combined.pdf"
    plt.savefig(plot_path, bbox_inches="tight", pad_inches=0)
    print("Wrote file to", plot_path)

    plt.close('all')

