import geopandas as gpd
import shapely.wkt
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns


bqclient = bigquery.Client.from_service_account_json(
    "dataverbinders-dev-32f411a93909.json"
)

for i in bqclient.list_tables("bag"):
    print(i.table_id)


# kadaster.Woonplaats
qres = bqclient.query(
    """
    SELECT
        *
    FROM
        `dataverbinders-dev.bag.Woonplaats`
    WHERE
        Objecten_naam = 'Dordrecht'
               """
)

df = qres.to_dataframe()
geo_col = []
for r in df.geometry:
    geostring = (
        r["geo_polygon"] if r["geo_polygon"] else r["geo_multi_polygon"]
    )
    geo = shapely.wkt.loads(geostring)
    print(geo)
    print(type(geo))
    geo_col.append(geo)

#  df.drop("geometry", axis=1)
#  geodf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry=geo_col)
#  print(geodf)
#  geodf.boundary.plot()
#  plt.gca().invert_yaxis()
#  plt.savefig("test.svg")
