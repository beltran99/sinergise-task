import json
import math
import datetime as dt
import calendar
from pathlib import Path
from typing import Literal

import geojson
from shapely.geometry import shape
from rasterio.io import MemoryFile
from rasterio.merge import merge
from sentinelhub import BBox, CRS, bbox_to_dimensions

from src import RESOLUTION, MAX_TILE_SIZE

def get_AOI_shape(aoi_path: Path) -> shape:
    """Reads a GeoJSON file and returns a geometry object.

    Args:
        aoi_path (Path): Path to the GeoJSON file defining the area of interest.

    Returns:
        shape: Resulting geometry object.
    """
    with open(aoi_path) as f:
        gj = geojson.load(f)
        
    return shape(gj)

def geojson_to_bbox(geojson_path: Path) -> list[float]:
    """Reads a GeoJSON file and returns its bounding box in [minx, miny, maxx, maxy] format.

    Args:
        geojson_path (Path): Path to the GeoJSON file defining the area of interest.

    Returns:
        list[float]: Bounding box in [minx, miny, maxx, maxy] format.
    """
    shape_obj = get_AOI_shape(geojson_path)
    minx, miny, maxx, maxy = shape_obj.bounds
    
    return [minx, miny, maxx, maxy]

def get_number_of_tiles(bbox: BBox, resolution: int = RESOLUTION, max_size: int = MAX_TILE_SIZE) -> tuple[int, int]:
    """Computes the number of tiles needed to cover the given bounding box, resolution and maximum tile size.

    Args:
        bbox (BBox): Bounding box object defining the area of interest.
        resolution (int, optional): Resolution in meters. Defaults to RESOLUTION.
        max_size (int, optional): Maximum tile size in pixels. Defaults to MAX_TILE_SIZE.

    Returns:
        tuple[int, int]: Number of tiles needed in the x and y directions.
    """
    bbox_size = bbox_to_dimensions(bbox, resolution=resolution)
    nx = math.ceil(bbox_size[0] / max_size)
    ny = math.ceil(bbox_size[1] / max_size)
    
    return nx, ny

def merge_tiles(download_responses: list, response_type: Literal["sentinel", "CLMS"], bbox: BBox) -> MemoryFile:
    """Reads into memory the downloaded tiles and merges them into a single in-memory raster mosaic.

    Args:
        download_responses (list): List of tile download responses from Sentinel Hub API.
        response_type (Literal["sentinel", "CLMS"]): Type of the downloaded response, either 'sentinel' or 'CLMS'.
        bbox (BBox): Bounding box object defining the area of interest.

    Returns:
        MemoryFile: In-memory raster mosaic of the merged tiles.
    """
    datasets = []
    for download_response in download_responses:
        memfile = MemoryFile(download_response.content)
        width, height = None, None
        if response_type == "sentinel":
            width = download_response.request.post_values['output']['width']
            height = download_response.request.post_values['output']['height']
        elif response_type == "CLMS":
            width = json.loads(download_response.request.body.decode('utf-8'))['output']['width']
            height = json.loads(download_response.request.body.decode('utf-8'))['output']['height']
        ds = memfile.open(width=width, height=height, crs='EPSG:4326')
        datasets.append(ds)
    
    mosaic, transform = merge(datasets, bounds=bbox.geometry.bounds)
    
    profile = datasets[0].profile.copy()
    profile.update(
        height=mosaic.shape[1],
        width=mosaic.shape[2],
        transform=transform
    )
    memfile = MemoryFile()
    with memfile.open(**profile) as mem_dataset:
        mem_dataset.write(mosaic)

    for ds in datasets:
        ds.close()
        
    return memfile

def has_dates_around_target(dates: list[dt.datetime], target: dt.datetime) -> bool:
    """Checks if in the given list there are dates both before and after a target date.

    Args:
        dates (list[dt.datetime]): List of datetime objects.
        target (dt.datetime): Target datetime object.

    Returns:
        bool: True if there are dates both before and after the target date, False otherwise.
    """
    has_smaller = any(d < target for d in dates)
    has_larger = any(d > target for d in dates)
    
    return has_smaller and has_larger
    
def get_dekadal_targets(start_date: dt.datetime, end_date: dt.datetime) -> list[str]:
    """Computes a list of dekadal target dates (1st, 11th, and 21st of each month) within a given time interval.

    Args:
        start_date (dt.datetime): Start datetime object of the interval.
        end_date (dt.datetime): End datetime object of the interval.

    Returns:
        list[str]: List of dekadal target dates in the interval.
    """
    targets = []
    year, month = start_date.year, start_date.month
    while (year < end_date.year) or (year == end_date.year and month <= end_date.month):
        
        # fixed dekads
        dekads = [1, 11, 21]
        
        for d in dekads:
            d = dt.datetime(year, month, d, hour=0, minute=0, second=0)
            if d >= start_date and d <= end_date:
                d_iso = f"{d.date().isoformat()}T{d.time().isoformat()}Z"
                targets.append(d_iso)
        
        # move one month
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    
    return targets