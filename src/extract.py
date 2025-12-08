import json
import datetime as dt
import logging
from pathlib import Path

import numpy as np
import xarray as xr
from rasterio.io import MemoryFile
from sentinelhub import (CRS, BBox, BBoxSplitter,
                         DataCollection, MimeType, SentinelHubCatalog,
                         SentinelHubDownloadClient, SentinelHubRequest,
                         SHConfig, bbox_to_dimensions)
from sentinelhub.api.catalog import CatalogSearchIterator

from src import RESOLUTION
from src.utils import get_AOI_shape, get_number_of_tiles, merge_tiles, get_dekadal_targets, has_dates_around_target
from src.logger import get_logger

ROOT_DIR = Path(__file__).parent.parent.resolve()
EVALSCRIPT_PATH = ROOT_DIR / "src/evalscript.js"
glogger = None

def setup_sh_config() -> SHConfig:
    """_summary_

    Returns:
        SHConfig: _description_
    """
    cfg_path = ROOT_DIR / "config.json"
    with open(cfg_path) as json_data_file:
        cfg = json.load(json_data_file)
        
    glogger.info(f"Setting up Sentinel Hub configuration from {cfg_path}.")

    config = SHConfig()
    config.sh_client_id = cfg["sentinelhub"]["client_id"]
    config.sh_client_secret = cfg["sentinelhub"]["client_secret"]
    config.save()
    
    return config

def search(config: SHConfig, bbox: BBox, target_interval: tuple[str, str], search_interval: tuple[str, str]) -> CatalogSearchIterator:
    """_summary_

    Args:
        config (SHConfig): _description_
        bbox (BBox): _description_
        target_interval (tuple[str, str]): _description_
        search_interval (tuple[str, str]): _description_

    Returns:
        CatalogSearchIterator: _description_
    """
    # Consult Sentinel2 L2A data
    catalog = SentinelHubCatalog(config=config)
    results = catalog.search(
        DataCollection.SENTINEL2_L2A,
        bbox=bbox,
        time=search_interval,
        fields={"include": ["id", "properties.datetime", "properties.eo:cloud_cover"], "exclude": []},
        filter="eo:cloud_cover < 10"
    )
    result_times = [dt.datetime.fromisoformat(result['properties']['datetime']) for result in results]
    
    # Compare search result times with expected dekadal interpolation targets
    start = dt.datetime.fromisoformat(search_interval[0])
    end = dt.datetime.fromisoformat(search_interval[1])
    targets = [
        dt.datetime.fromisoformat(t)
        for t in get_dekadal_targets(
            start,
            end
        )
    ]
    
    # If there is no data around a dekadal target, interpolation cannot
    # be performed for that point and thus the search interval needs to be extended.
    # Note: only first and last dekadal targets are checked, since
    # search interval can only be extended on the edges
    valid_results = [False, False]       
    for i, target in enumerate([targets[0], targets[-1]]):
        if has_dates_around_target(result_times, target):
            valid_results[i] = True

    if all(valid_results):
        return results
    
    # Adjust search interval and re-search until valid results are found
    if not valid_results[0]:
        glogger.info("Not enough data before the start target date, adjusting search interval.")
        start -= dt.timedelta(days=10)
    if not valid_results[1]:
        glogger.info("Not enough data after the end target date, adjusting search interval.")
        end += dt.timedelta(days=10)
        
    glogger.info(f"Adjusted time interval for search: {(start.isoformat(), end.isoformat())}")
    return search(config, bbox, target_interval, (start.isoformat(), end.isoformat()))

def get_sh_request(evalscript: str, responses: list, time_interval: tuple[str, str], bbox: BBox, bbox_size: tuple[int, int]) -> SentinelHubRequest:
    """_summary_

    Args:
        evalscript (str): _description_
        responses (list): _description_
        time_interval (tuple[str, str]): _description_
        bbox (BBox): _description_
        bbox_size (tuple[int, int]): _description_

    Returns:
        SentinelHubRequest: _description_
    """
    input_data = [
    SentinelHubRequest.input_data(
        data_collection=DataCollection.SENTINEL2_L2A,
        time_interval=time_interval,
        mosaicking_order='leastCC'
        )
    ]
    return SentinelHubRequest(
        data_folder=ROOT_DIR / "data/raw",
        evalscript=evalscript,
        input_data=input_data,
        responses=responses,
        bbox=bbox,
        size=bbox_size,
    )

def download(aoi: Path, config: SHConfig, search_results: CatalogSearchIterator, bbox: BBox, time_interval: tuple[str, str]) -> tuple[list, np.ndarray]:
    """_summary_

    Args:
        aoi (Path): _description_
        config (SHConfig): _description_
        search_results (CatalogSearchIterator): _description_
        bbox (BBox): _description_
        time_interval (tuple[str, str]): _description_

    Returns:
        tuple[list, np.ndarray]: _description_
    """
    bbox_size = bbox_to_dimensions(bbox, resolution=RESOLUTION)
    nx, ny = get_number_of_tiles(bbox, resolution=RESOLUTION)
    
    glogger.info(f"AOI size in pixels: {bbox_size}")

    if nx > 1 or ny > 1:
        area = get_AOI_shape(aoi)
        
        bbox_splitter = BBoxSplitter(
            [area],
            crs=CRS.WGS84,
            split_shape=(nx, ny)
        )
        
        tiles = bbox_splitter.get_bbox_list()
        tile_sizes = [bbox_to_dimensions(tile, resolution=RESOLUTION) for tile in tiles]
    else:
        tiles = [bbox]
        tile_sizes = [bbox_size]    
    
    start = dt.datetime.fromisoformat(time_interval[0])
    end = dt.datetime.fromisoformat(time_interval[1])
    targets = get_dekadal_targets(start, end)
    # convert targets to numpy datetime64 for xarray compatibility
    target_times = np.array([np.datetime64(target[:-1]) for target in targets]) 
    
    glogger.info(f"Number of dekadal targets: {len(targets)}.")
    glogger.info(f"Dekadal targets: {list(target_times)}")
     
    # Evalscript
    with open(EVALSCRIPT_PATH) as fp:
        evalscript = fp.read()
        
    # Responses
    responses = [SentinelHubRequest.output_response("default", MimeType.TIFF)]
    
    # Create requests
    requests = []
    for search_result in search_results:
        for tile, tile_size in zip(tiles, tile_sizes):
            time_interval = (search_result['properties']['datetime'], search_result['properties']['datetime'])
            request = get_sh_request(evalscript, responses, time_interval, tile, tile_size)
            requests.append(request)

    # Download data
    glogger.info(f"Total number of SentinelHub requests: {len(requests)}. Starting download...")
    show_progress = True if glogger.level <= logging.INFO else False
    download_responses = SentinelHubDownloadClient(config=config, redownload=False).download(
        [request.download_list[0] for request in requests],
        max_threads=5,
        show_progress=show_progress,
        decode_data=False
    )
    glogger.info(f"Download completed. Number of responses: {len(download_responses)}")
    
    return download_responses, target_times

def process_responses(download_responses: list, search_results: CatalogSearchIterator, bbox: BBox) -> list[xr.DataArray]:
    """_summary_

    Args:
        download_responses (list): _description_
        search_results (CatalogSearchIterator): _description_
        bbox (BBox): _description_

    Returns:
        list[xr.DataArray]: _description_
    """
    
    xarrays = []
    for i, search_result in enumerate(search_results):
        
        time = search_result['properties']['datetime']
        
        # gets all tiles for this search result
        response = [resp for resp in download_responses
                    if resp.request.post_values['input']['data'][0]['dataFilter']['timeRange']['from'] == time
                ]
        
        raster_file = None
        if len(response) > 1:
            raster_file = merge_tiles(response, bbox)
        else:
            raster_file = MemoryFile(response[0].content)
            
        with raster_file.open() as ds_rio:
            xarray = xr.open_dataarray(ds_rio, engine="rasterio").squeeze(drop=True)
            xarray = xarray.expand_dims("time")
            xarray = xarray.assign_coords(time=[np.datetime64(time[:-1])]) # numpy does not support timezone
            xarray.name = "NDVI"
            xarrays.append(xarray)
            
    return xarrays

def extract(bbox: BBox, time_interval: tuple[str, str], aoi: Path, logger: logging.Logger) -> tuple[list[xr.DataArray], np.ndarray]:
    """_summary_

    Args:
        bbox (BBox): _description_
        time_interval (tuple[str, str]): _description_
        aoi (Path): _description_
        logger (logging.Logger): _description_

    Returns:
        tuple[list[xr.DataArray], np.ndarray]: _description_
    """
    global glogger
    glogger = logger
    
    config = setup_sh_config()
    
    search_results = search(
        config,
        bbox=bbox,
        target_interval=time_interval,
        search_interval=time_interval,
    )
    logger.info(f"Total number of search results from Catalog API: {len(list(search_results))}")
    
    for i, search_result in enumerate(search_results):
        logger.info(f"Search result {i}: {search_result}")
        
    download_responses, target_times = download(
        aoi,
        config,
        search_results=search_results,
        bbox=bbox,
        time_interval=time_interval
    )
    
    # Process responses into xarrays
    logger.info("Processing downloaded data into xarrays...")
    xarrays = process_responses(download_responses, search_results, bbox)
    
    return xarrays, target_times