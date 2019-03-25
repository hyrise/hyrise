import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

fig, ax = plt.subplots()

df = pd.read_csv("out.csv", names=['identifier','time'])
colors = np.where(df["identifier"] == 'D','r','-')
colors[df["identifier"] == 'N'] = 'g'
colors[df["identifier"] == 'O'] = 'b'
colors[df["identifier"] == 'P'] = 'c'
colors[df["identifier"] == 'S'] = 'm'
df.reset_index().plot(kind='scatter', x='index', y='time', c=colors)

handles = [
	mpatches.Patch(color='r', label='Delivery'),
	mpatches.Patch(color='g', label='NewOrder'),
	mpatches.Patch(color='b', label='OrderStatus'),
	mpatches.Patch(color='c', label='Payment'),
	mpatches.Patch(color='m', label='StockLevel')
]
plt.legend(handles=handles)

ax.grid(True)

plt.savefig('tpcc.pdf')