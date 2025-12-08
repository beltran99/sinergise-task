import json
import datetime as dt
import logging
from pathlib import Path

import numpy as np
import xarray as xr
from rasterio.io import MemoryFile
from sentinelhub import (CRS, BBox, BBoxSplitter,
                         MimeType, SentinelHubCatalog,
                         SentinelHubDownloadClient, SentinelHubRequest,
                         SHConfig, bbox_to_dimensions)
from sentinelhub.api.catalog import CatalogSearchIterator

from src import RESOLUTION, DATA_COLLECTION
from src.utils import get_AOI_shape, get_number_of_tiles, merge_tiles, get_dekadal_targets, has_dates_around_target
from src.logger import get_logger

ROOT_DIR = Path(__file__).parent.parent.parent.resolve()
EVALSCRIPT_PATH = ROOT_DIR / "src/evalscript.js"
glogger = None

def setup_sh_config() -> SHConfig:
    """Sets up Sentinel Hub configuration from config.json file.
    Config file is expected to be named 'config.json', be located
    in the root directory of the project, and contain the following structure:
    {
        "sentinelhub": {
            "client_id": "<your_client_id>",
            "client_secret": "<your_client_secret>"
        }
    }

    Returns:
        SHConfig: Sentinel Hub configuration object
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

def search(config: SHConfig, bbox: BBox, search_interval: tuple[str, str]) -> CatalogSearchIterator:
    """Queries Sentinel Hub catalog for Sentinel-2 L2A data within a specified bounding box and time intervals.
    The data is filtered by a cloud cover less than 10%, to ensure good quality for NVDI composite generation.
    The search interval is automatically adjusted to ensure that there is enough data for interpolation around dekadal target dates.

    Args:
        config (SHConfig): Sentinel Hub configuration object
        bbox (BBox): Bounding box object defining the area of interest
        search_interval (tuple[str, str]): Search time interval for querying data in (start, end) format

    Returns:
        CatalogSearchIterator: Sentinel Hub CatalogSearchIterator object containing the search results
    """
    # Consult Sentinel2 L2A data
    catalog = SentinelHubCatalog(config=config)
    results = catalog.search(
        DATA_COLLECTION,
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
    return search(config, bbox, (start.isoformat(), end.isoformat()))

def get_sh_request(evalscript: str, responses: list, time_interval: tuple[str, str], bbox: BBox, bbox_size: tuple[int, int]) -> SentinelHubRequest:
    """Builds a SentinelHubRequest object for downloading data.

    Args:
        evalscript (str): Evalscript defining the data to be retrieved
        responses (list): List of response configurations
        time_interval (tuple[str, str]): Time interval of interest in (start, end) format
        bbox (BBox): Bounding box object defining the area of interest
        bbox_size (tuple[int, int]): Size of the bounding box in pixels in (width, height)

    Returns:
        SentinelHubRequest: SentinelHubRequest object configured for data download
    """
    input_data = [
    SentinelHubRequest.input_data(
        data_collection=DATA_COLLECTION,
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
    """Downloads data from Sentinel Hub based on the provided search results, bounding box, and time interval.
    Steps:
    1. Determine if the bounding box needs to be split into smaller tiles based on its size and the maximum tile size.
    2. Generate dekadal target dates within the specified time interval for which data is to be downloaded.
    3. Create SentinelHubRequest objects for each search result and tile.
    4. Download the data using SentinelHubDownloadClient.

    Args:
        aoi (Path): Path to the GeoJSON file defining the area of interest
        config (SHConfig): Sentinel Hub configuration object
        search_results (CatalogSearchIterator): Sentinel Hub CatalogSearchIterator object containing the catalog items to be downloaded
        bbox (BBox): Bounding box object defining the area of interest
        time_interval (tuple[str, str]): Time interval of interest in (start, end) format

    Returns:
        tuple[list, np.ndarray]: Tuple containing the download responses and the dekadal target times
    """
    
    ### 1. Split bbox into tiles if necessary ###
    bbox_size = bbox_to_dimensions(bbox, resolution=RESOLUTION)
    nx, ny = get_number_of_tiles(bbox, resolution=RESOLUTION)

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
        
    extra_info = f" Need to split bounding box with split shape ({nx}, {ny})" if (nx > 1 or ny > 1) else ""
    glogger.info(f"AOI size in pixels: {bbox_size}.{extra_info}")
    
    ### 2. Calculate dekadal target dates ###
    start = dt.datetime.fromisoformat(time_interval[0])
    end = dt.datetime.fromisoformat(time_interval[1])
    targets = get_dekadal_targets(start, end)
    # convert targets to numpy datetime64 for xarray compatibility
    target_times = np.array([np.datetime64(target[:-1]) for target in targets]) 
    
    glogger.info(f"Number of dekadal targets: {len(targets)}. Dekadal targets: {[str(t.astype('datetime64[D]')) for t in target_times]}")
     
    ### 3. Define SentinelHubRequest objects ###
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

    ### 4. Download data ###
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
    """Process raw Sentinel Hub responses into xarray DataArrays.

    Args:
        download_responses (list): List of Sentinel Hub download responses.
        search_results (CatalogSearchIterator): Sentinel Hub CatalogSearchIterator object containing the downloaded catalog items.
        bbox (BBox): Bounding box defining the area of interest.

    Returns:
        list[xr.DataArray]: List of xarray DataArrays containing the downloaded NDVI data.
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
    """Extracts NDVI data from Sentinel Hub for a given bounding box and time interval.
    Steps:
    1. Set up Sentinel Hub configuration.
    2. Fetch Sentinel Hub catalog data within the specified search parameters.
    3. Download the raw data.
    4. Process the raw data into xarray DataArrays.

    Args:
        bbox (BBox): Bounding box defining the area of interest.
        time_interval (tuple[str, str]): Time interval of interest in (start, end) format.
        aoi (Path): Path to the GeoJSON file defining the area of interest.
        logger (logging.Logger): Logger object for logging information.

    Returns:
        tuple[list[xr.DataArray], np.ndarray]: Tuple containing a list of NDVI DataArrays and dekadal targets.
    """
    global glogger
    glogger = logger
    
    ### 1. Setup Sentinel Hub configuration ###
    config = setup_sh_config()
    
    ### 2. Search catalog for relevant data ###
    search_results = search(
        config,
        bbox=bbox,
        search_interval=time_interval,
    )
    logger.info(f"Total number of search results from Catalog API: {len(list(search_results))}")
    
    for i, search_result in enumerate(search_results):
        logger.info(f"Search result {i}: {search_result}")
        
    ### 3. Download data ###
    download_responses, target_times = download(
        aoi,
        config,
        search_results=search_results,
        bbox=bbox,
        time_interval=time_interval
    )
    
    ### 4. Process responses into xarrays ###
    logger.info("Processing downloaded data into xarrays...")
    xarrays = process_responses(download_responses, search_results, bbox)
    
    return xarrays, target_times