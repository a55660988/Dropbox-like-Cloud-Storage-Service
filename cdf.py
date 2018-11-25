import numpy as np
import matplotlib.pyplot as plt


data_s_hash = [1.88, 2.01, 1.66, 1.65, 1.87, 2.02, 1.66, 2.00, 1.69, 2.00, 1.66, 2.00, 1.99, 1.66, 2.02, 1.69, 2.00, 1.68, 1.89, 1.90, 1.89, 1.66, 1.89, 1.89, 1.65]
data_s_near = [1.66, 1.66, 1.66, 1.66, 1.66, 1.66, 1.66, 1.66, 1.66, 1.66, 1.65, 1.65, 1.66, 1.65, 1.66, 1.66, 1.66, 1.66, 1.65, 1.66, 1.66, 1.65, 1.65, 1.66, 1.66]

data_b_hash = [7.75, 8.64, 7.94, 7.72, 8.21, 7.81, 8.05, 7.36, 8.10, 7.54, 7.16, 7.48, 8.15, 8.33, 8.06, 7.41, 7.16, 7.72, 8.71, 8.05, 7.77, 8.02, 7.28, 8.87, 7.80]
data_b_near = [7.02, 7.00, 7.02, 7.02, 7.07, 7.02, 7.01, 7.01, 7.03, 7.02, 7.05, 7.02, 7.06, 7.03, 7.03, 7.05, 7.01, 7.03, 7.04, 7.02, 7.02, 7.01, 7.02, 7.02, 7.03]

num_bins = 50

counts_hash, bin_edges_hash = np.histogram (data_s_hash, bins=num_bins, normed=True)
cdf_hash = np.cumsum (counts_hash)

counts_near, bin_edges_near = np.histogram (data_s_near, bins=num_bins, normed=True)
cdf_near = np.cumsum (counts_near)


# plot the cdf
plt.plot(bin_edges_hash[1:], cdf_hash/cdf_hash[-1], "r", label="small_hash")
plt.plot(bin_edges_near[1:], cdf_near/cdf_near[-1], "g", label="small_near")
# plt.plot(bin_edges_hash[1:], cdf_hash/cdf_hash[-1], "r", label="big_hash")
# plt.plot(bin_edges_near[1:], cdf_near/cdf_near[-1], "g", label="big_near")

print("average of small hash: ", sum(data_s_hash) / len(data_s_hash))
print("average of small near: ", sum(data_s_near) / len(data_s_near))
print("average of big hash: ", sum(data_b_hash) / len(data_b_hash))
print("average of big near: ", sum(data_b_near) / len(data_b_near))

plt.legend()
plt.show()

