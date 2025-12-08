import math
import datetime as dt
import calendar
from pathlib import Path

import geojson
from shapely.geometry import shape
from rasterio.io import MemoryFile
from rasterio.merge import merge
from sentinelhub import BBox, CRS, bbox_to_dimensions

from src import RESOLUTION, MAX_TILE_SIZE

def get_AOI_shape(aoi_path: Path) -> shape:
    """_summary_

    Args:
        aoi_path (Path): _description_

    Returns:
        shape: _description_
    """
    with open(aoi_path) as f:
        gj = geojson.load(f)
        
    return shape(gj)

def geojson_to_bbox(geojson_path: Path) -> list[float]:
    """_summary_

    Args:
        geojson_path (Path): _description_

    Returns:
        list[float]: _description_
    """
    shape_obj = get_AOI_shape(geojson_path)
    minx, miny, maxx, maxy = shape_obj.bounds
    
    return [minx, miny, maxx, maxy]

def get_number_of_tiles(bbox: BBox, resolution: int = RESOLUTION, max_size: int = MAX_TILE_SIZE) -> tuple[int, int]:
    """_summary_

    Args:
        bbox (BBox): _description_
        resolution (int, optional): _description_. Defaults to RESOLUTION.
        max_size (int, optional): _description_. Defaults to MAX_TILE_SIZE.

    Returns:
        tuple[int, int]: _description_
    """
    bbox_size = bbox_to_dimensions(bbox, resolution=resolution)
    nx = math.ceil(bbox_size[0] / max_size)
    ny = math.ceil(bbox_size[1] / max_size)
    
    return nx, ny

def merge_tiles(download_responses: list, bbox: BBox) -> MemoryFile:
    """_summary_

    Args:
        download_responses (list): _description_
        bbox (BBox): _description_

    Returns:
        MemoryFile: _description_
    """
    datasets = []
    for download_response in download_responses:
        memfile = MemoryFile(download_response.content)
        width = download_response.request.post_values['output']['width']
        height = download_response.request.post_values['output']['height']
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
    """_summary_

    Args:
        dates (list[dt.datetime]): _description_
        target (dt.datetime): _description_

    Returns:
        bool: _description_
    """
    has_smaller = any(d < target for d in dates)
    has_larger = any(d > target for d in dates)
    
    return has_smaller and has_larger
    
def get_dekadal_targets(start_date: dt.datetime, end_date: dt.datetime) -> list[str]:
    """_summary_

    Args:
        start_date (dt.datetime): _description_
        end_date (dt.datetime): _description_

    Returns:
        list[str]: _description_
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