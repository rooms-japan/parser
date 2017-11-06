# coding:utf-8
# 万 = \u4e07
# 円 = \u5186

# TODO : gérer quartier
# TODO: Faire un pdf de courbes (matplotlib ?)
# TODO : load from txt
from parsers import *

from mpl_toolkits.mplot3d import Axes3D
from matplotlib import pyplot as plt

# agharta = Agharta("https://www.realestate.co.jp/agharta/ja/rent/listing?page=1&prefecture=JP-13")
# agharta = Agharta("https://apartments.gaijinpot.com/ja/rent/listing?prefecture=JP-13&max_price=150000&pets=1")
# agharta.parse()
# agharta.print("data-sample.txt")
agharta.load("data.txt")

# Make a plot ?
# key1 = "surface"
key1 = "surface"
key2 = "rent"
color = "walk_time"
data_filter = [(x,y,c) for (x,y,c) in zip(agharta.get_key(key1), agharta.get_key(key2), agharta.get_key(color))] 
x, y, c = zip(*data_filter)
wards = list(set(c))
# ipdb.set_trace()
from sklearn.datasets import load_iris
iris = load_iris()

# use plt.cm.get_cmap(cmap, N) to get an N-bin version of cmap
# plt.scatter(iris.data[:, 0], iris.data[:, 1], s=30,
plt.scatter(x, y, c=[c.index(x) for x in c], cmap=plt.cm.get_cmap('ocean', len(wards)))

# This function formatter will replace integers with target names
formatter = plt.FuncFormatter(lambda val, loc: wards[val])

# We must be sure to specify the ticks matching our target names
plt.colorbar(ticks=[i for i in range(0, len(set(c)))], format=formatter);

# Set the clim so that labels are centered on each block
plt.clim(-0.5, 22.5)

# plt.show()


# plt.scatter(x, y, c=c, marker='o')
plt.xlabel(key1)
plt.ylabel(key2)
# plt.colorbar(cmap=c)
plt.savefig('surface-rent.eps', format='eps', dpi=300)
