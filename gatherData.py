import requests
from shapely.geometry import shape
import os
from mpi4py import MPI
from utils import getBoundaries
import time

def gatherData(osmIds, polygons):
  chunkSize = 5
  path = "./overpass_data"

  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  procSize = comm.Get_size()

  if rank == 0:
    if not os.path.exists(path):
      os.mkdir(path)

  osmBoundaries = getBoundaries(procSize, rank, len(osmIds))
  for osmIndex in range(osmBoundaries['my_start'], osmBoundaries['my_end']):
    osmId = osmIds[osmIndex]
    i = 1
    polygon = shape(polygons[osmId])
    polygon_all_x = []
    polygon_all_y = []
    if polygons[osmId]['type'] == 'Polygon':
      polygon_all_y = polygon.exterior.xy[0]
      polygon_all_x = polygon.exterior.xy[1]
    elif polygons[osmId]['type'] == 'MultiPolygon':
      for geom in polygon.geoms:
        polygon_all_y.extend(geom.exterior.xy[0])
        polygon_all_x.extend(geom.exterior.xy[1])

    min_x = min(polygon_all_x)
    min_y = min(polygon_all_y)
    max_x = max(polygon_all_x)
    max_y = max(polygon_all_y)

    x_iter = (max_x - min_x) / chunkSize
    y_iter = (max_y - min_y) / chunkSize

    for x_i in range(0, chunkSize):
      for y_i in range(0, chunkSize):
        small_min_x = min_x + x_i * x_iter
        small_min_y = min_y + y_i * y_iter
        small_max_x = min_x + (x_i + 1) * x_iter
        small_max_y = min_y + (y_i + 1) * y_iter

        while True:
          url = f"https://overpass-api.de/api/map?bbox={small_min_y},{small_min_x},{small_max_y},{small_max_x}"
          r = requests.get(url)
          if r.status_code == 200:
            data = r.content
            break;
          time.sleep(5)

        filename = f'{osmId}_{i}'
        filepath = f'{path}/{filename}'
        if os.path.exists(filepath):
          os.remove(filepath)
        with open(filepath, 'wb') as f:
            f.write(data)
        i += 1