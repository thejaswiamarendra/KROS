import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv("values.csv")

df = df.sort_values(by=['bytes'])
print(df)
#plt.scatter(np.array(df["bytes"])[:, None], np.array(df["time"])[:, None])
x = np.array(df["bytes"])[:, None]
y = np.array(df["time"])[:, None]
plt.scatter(x, y)
# add axes labels
plt.xlabel('Payload(Bytes)')
plt.ylabel('Latency(ms)')
# add labels to all points
plt.plot(x, y)

plt.show()
