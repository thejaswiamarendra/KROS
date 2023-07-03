import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv("values.csv")
print(df["time"])
plt.scatter(np.array(df["bytes"])[:, None], np.array(df["time"])[:, None])
plt.show()
