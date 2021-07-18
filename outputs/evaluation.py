import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math
from matplotlib.ticker import FormatStrFormatter

def load_file(name: str) -> pd.DataFrame:
    df = pd.read_csv(f"measurements_q_{name}.txt")
    df['table'] = name
    df = df.rename(columns=lambda x: x.strip())
    return df

def load_files():
    tables = ["lineitem", "part", "supplier", "partsupp", "customer", "orders", "nation", "region"]
    # df = pd.DataFrame()
    df = pd.concat(map(load_file, tables), axis = 0)
    # for table in tables:
    #   new_df = pd.read_csv("measurements_q_" + table + ".txt")
    #   df.append(new_df, ignore_index=True)
    return df

def plot_relative_times():
    df = load_files()
    # print(df)

    # set width of bars
    barWidth = 0.2
    number_of_rows = df.shape[0]
    # print(number_of_rows)
    # set heights of bars (full_hist, merge_hist_with_hlls, merge_hist_without_hlls)
    bars1 = df['time_full_histogram'] / df['time_full_histogram']
    bars2 = df['time_merging_histograms_hlls'] / df['time_full_histogram']
    bars3 = df['time_merging_histograms_without_hlls'] / df['time_full_histogram']
    bars4 = df['time_per_segment_histograms'] / df['time_full_histogram']
    
    # Set position of bar on X axis
    r1 = np.arange(number_of_rows)
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]
    r4 = [x + barWidth for x in r3]
    
    # Make the plot

    plt.figure(figsize = (30, 6))
    plt.bar(r1, bars2, color='#f6A800', width=barWidth, edgecolor='white', label='Merging with HLLs')
    plt.bar(r2, bars3, color='#dd6108', width=barWidth, edgecolor='white', label='Merging without HLLs')
    plt.bar(r3, bars4, color='#b1063a', width=barWidth, edgecolor='white', label='Creating Per-Segment Histograms')
    # plt.bar(r4, bars4, color='#5a6065', width=barWidth, edgecolor='white', label='per_segment_his')
    
    # Add xticks on the middle of the group bars
    plt.xlabel('column', fontweight='bold', fontsize=20)
    plt.xticks([r + barWidth for r in range(len(bars1))], df['table'] + df['column_id'].apply(lambda x: f"[{x}]\n") + df['column_type'].apply(lambda x: f"({x.strip()})"))
    plt.ylabel('relative time', fontweight='bold', fontsize=20)
    plt.yscale("log")
    xlimits = plt.xlim()
    plt.hlines(1.0, plt.xlim()[0], plt.xlim()[1], color="#5a6065", linestyles = '--')
    plt.xlim(xlimits)
    plt.gca().yaxis.set_major_formatter(FormatStrFormatter('% 1.3f'))

    # Create legend & Show graphic
    plt.legend(fontsize = 17)
    plt.savefig("times.png")
    plt.show()
    return df


def plot_errors():
    df = load_files()
    # print(df)

    # set width of bars
    barWidth = 0.2
    number_of_rows = df.shape[0]
    # print(number_of_rows)
    # set heights of bars (full_hist, merge_hist_with_hlls, merge_hist_without_hlls)
    bars1 = df['error_full'] #/ df['error_full']
    bars2 = df['error_merged_hll'] #/ (df['error_full'] + 0.0000001)
    bars3 = df['error_merged_without_hll'] #/ (df['error_full'] + 0.0000001)

    #maxheight = max((df['error_merged_hll'] / df['error_full']).apply(lambda x: 0.0 if (math.isnan(x) or math.isinf(x)) else x).max(),(df['error_merged_hll'] / df['error_full']).apply(lambda x: 0.0 if (math.isnan(x) or math.isinf(x)) else x).max())
    
    # Set position of bar on X axis
    r1 = np.arange(number_of_rows)
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]
    
    # Make the plot

    plt.figure(figsize = (30, 6))
    plt.bar(r1, bars2, color='#f6A800', width=barWidth, edgecolor='white', label='Merging with HLLs')
    plt.bar(r2, bars3, color='#dd6108', width=barWidth, edgecolor='white', label='Merging without HLLs')
    plt.bar(r3, bars1, color='#b1063a', width=barWidth, edgecolor='white', label='Full Histogram')
    # plt.bar(r4, bars4, color='#5a6065', width=barWidth, edgecolor='white', label='per_segment_his')
    
    # Add xticks on the middle of the group bars
    plt.xlabel('column', fontweight='bold', fontsize=20)
    plt.xticks([r + barWidth for r in range(len(bars1))], df['table'] + df['column_id'].apply(lambda x: f"[{x}]\n") + df['column_type'].apply(lambda x: f"({x.strip()})"))
    plt.ylabel('relative error', fontweight='bold', fontsize=20)
    plt.yscale("log")
    xlimits = plt.xlim()
    plt.hlines(1.0, plt.xlim()[0], plt.xlim()[1], color="#5a6065", linestyles = '--')
    plt.xlim(xlimits)
    #plt.ylim(0.0, maxheight*1.1)
    plt.gca().yaxis.set_major_formatter(FormatStrFormatter('% 1.5f'))

    # Create legend & Show graphic
    plt.legend(fontsize = 17)
    plt.savefig("errors.png")
    plt.show()
    return df


def main():
    return plot_errors()



if __name__ == "__main__":
    main()