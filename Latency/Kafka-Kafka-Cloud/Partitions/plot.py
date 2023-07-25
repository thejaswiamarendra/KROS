'''import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv("values.csv")
print(df["time"])
plt.scatter(np.array(df["bytes"])[:, None], np.array(df["time"])[:, None])
plt.figsave('partitions1.png')'''

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

df1 = pd.read_csv("values.csv")
df2 = pd.read_csv("partition2/values.csv")
df3 = pd.read_csv("partition3/values.csv")
df4 = pd.read_csv("partition4/values.csv")
df5 = pd.read_csv("partition5/values.csv")
df6 = pd.read_csv("partition6/values.csv")
df7 = pd.read_csv("partition7/values.csv")
df8 = pd.read_csv("partition8/values.csv")
#print(df["time"])
#df = df[(np.abs(stats.zscore(df["time"])) < 3)]
#print(df["time"])
#plt.scatter(np.array(df["bytes"])[:, None], np.array(df["time"])[:, None])

df1.sort_values('bytes', inplace=True)
df2.sort_values('bytes', inplace=True)
df3.sort_values('bytes', inplace=True)
df4.sort_values('bytes', inplace=True)
df5.sort_values('bytes', inplace=True)
df6.sort_values('bytes', inplace=True)
df7.sort_values('bytes', inplace=True)
df8.sort_values('bytes', inplace=True)

# Plot each DataFrame with a label
#plt.plot(df1["bytes"], df1["time"], label='Partition 1')
#plt.plot(df2["bytes"], df2["time"], label='Partition 2')
#plt.plot(df3["bytes"], df3["time"], label='Partition 3')
#plt.plot(df4["bytes"], df4["time"], label='Partition 4')
#plt.plot(df5["bytes"], df5["time"], label='Partition 5')
#plt.plot(df6["bytes"], df6["time"], label='Partition 6')
#plt.plot(df7["bytes"], df7["time"], label='Partition 7')
#plt.plot(df8["bytes"], df8["time"], label='Partition 8')

plt.plot(df1.iloc[2:]["bytes"], df1.iloc[2:]["time"], label='Partition 1')
plt.plot(df2.iloc[2:]["bytes"], df2.iloc[2:]["time"], label='Partition 2')
plt.plot(df3.iloc[2:]["bytes"], df3.iloc[2:]["time"], label='Partition 3')
plt.plot(df4.iloc[2:]["bytes"], df4.iloc[2:]["time"], label='Partition 4')
plt.plot(df5.iloc[2:]["bytes"], df5.iloc[2:]["time"], label='Partition 5')
plt.plot(df6.iloc[2:]["bytes"], df6.iloc[2:]["time"], label='Partition 6')
plt.plot(df7.iloc[2:]["bytes"], df7.iloc[2:]["time"], label='Partition 7')
plt.plot(df8.iloc[2:]["bytes"], df8.iloc[2:]["time"], label='Partition 8')

# Add axes labels
plt.xlabel('Payload (Bytes)')
plt.ylabel('Latency (ms)')

# Display the legend
plt.legend()

# Save the plot to a file
plt.savefig('partitions2.png')
