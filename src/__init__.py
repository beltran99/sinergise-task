from sentinelhub import DataCollection

#---------------------------------------------------------------------------
#   Constants
#---------------------------------------------------------------------------

RESOLUTION = 10  # meters per pixel
MAX_TILE_SIZE = 2500  # max size of one tile in pixels

# Data collections
S2L2A_COLLECTION = DataCollection.SENTINEL2_L2A  # Sentinel-2 L2A data collection
CLMS_NDVI_V1 = "byoc-41f33765-18a0-4e1a-ade2-b4093254ce68" # NDVI 2014-2020 (raster 300 m), global, 10-daily data collection
CLMS_NDVI_V2 = "byoc-ab0e1e8e-508c-4faa-9b5b-c9c4734ef29e" # NDVI 2020-present (raster 300 m), global, 10-daily data collection