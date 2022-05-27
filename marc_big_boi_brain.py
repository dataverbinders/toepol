from datetime import datetime
from math import floor, sqrt
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType

import pandas as pd
from tqdm import tqdm

start = datetime.now()
i = 5
#  while True:
#  long = 3.2 + ( (7.22 - 3.2) / 32700 ) * (i % 32700)
#  lat = 50.75 + ( (53.7 - 50.75) / 44700 ) * floor(i / 32700)
#
#  #  if i % 1000 == 0:
#  #  print(i)
#
#  if i > 32700*44700:
#  print(f"{datetime.now() - start}")
#  break
#
#  i += 1

# @udf(returnType=FloatType())
def index_to_lat(index: int) -> float:
    return 50.75 + ((53.7 - 50.75) / 44700) * floor(index / 32700)


# @udf(returnType=FloatType())
def index_to_long(index: int) -> float:
    return 3.2 + ((7.22 - 3.2) / 32700) * (index % 32700)


def index_to_lat_long(index: int):
    return (index_to_lat(index), index_to_long(index))

def lat_long_to_index(lat: float, long: float) -> int:
    i_lat = (lat - 50.75) / ((53.7 - 50.75) / 44700)
    i_long = (long - 3.2) / ((7.22 - 3.2) / 32700)
    index = i_long + 32700 * i_lat

    return index


@udf(returnType=IntegerType())
def find_nearest_index(lat: float, long: float) -> int:
    approx_index = floor(lat_long_to_index(lat, long))

    nearest_index = None
    nearest_index_dist = float("inf")

    lat_offset = [0]
    if approx_index % 32700 != 0:
        lat_offset.append(-1)
    if approx_index % 32700 != 32699:
        lat_offset.append(1)

    long_offset = [0]
    if approx_index >= 32700:
        long_offset.append(-1)
    if approx_index <= (44700 * 32700 - 32700):
        long_offset.append(1)

    for lat_o in lat_offset:
        for long_o in long_offset:
            comp_index = approx_index + lat_o + long_o * 32700
            comp_lat, comp_long = index_to_lat_long(comp_index)

            dist = sqrt((lat - comp_lat) ** 2 + (long - comp_long) ** 2)
            if dist < nearest_index_dist:
                nearest_index = comp_index
                nearest_index_dist = dist

    return nearest_index


# for i in range(0, 10000000):
#     lat, long = index_to_lat_long(i)
#     found_index = find_nearest_index(lat, long)
#     if i != found_index:
#         print(i, found_index)
