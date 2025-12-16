import json
import datetime as dt
import logging
import pprint
from pathlib import Path
from typing import List
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests import Response
from typing import Literal

import numpy as np
import xarray as xr
from rasterio.io import MemoryFile
from sentinelhub import (CRS, BBox, BBoxSplitter,
                         MimeType, SentinelHubCatalog,
                         SentinelHubDownloadClient, SentinelHubRequest,
                         SHConfig, bbox_to_dimensions,
                         ByocCollection, SentinelHubBYOC,
                         DataCollection)
from sentinelhub.api.catalog import CatalogSearchIterator

from src import RESOLUTION, S2L2A_COLLECTION, CLMS_NDVI_V1, CLMS_NDVI_V2
from src.utils import get_AOI_shape, get_number_of_tiles, merge_tiles, get_dekadal_targets, has_dates_around_target
from src.logger import get_logger

ROOT_DIR = Path(__file__).parent.parent.parent.resolve()
EVALSCRIPT_PATH = ROOT_DIR / "src/evalscript.js"
glogger = None

def setup_sh_config(profile: Literal["sentinelhub", "cdse"] = "sentinelhub") -> SHConfig:
    """Sets up Sentinel Hub configuration from config.json file.
    Config file is expected to be named 'config.json', be located
    in the root directory of the project, and contain the following structure:
    {
        "sentinelhub": {
            "client_id": "<your_sentinelhub_client_id>",
            "client_secret": "<your_sentinelhub_client_secret>"
        },
        "cdse": {
            "client_id": "<your_cdse_client_id>",
            "client_secret": "<your_cdse_client_secret>",
            "base_url": "https://sh.dataspace.copernicus.eu/api/v1/",
            "token_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        }
    }
    
    Args:
        profile (Literal["sentinelhub", "cdse"]): Profile name for Sentinel Hub configuration ("sentinelhub" or "cdse"). Defaults to "sentinelhub".

    Returns:
        SHConfig: Sentinel Hub configuration object
    """
    cfg_path = ROOT_DIR / "config.json"
    with open(cfg_path) as json_data_file:
        cfg = json.load(json_data_file)
        
    glogger.info(f"Setting up Sentinel Hub configuration from {cfg_path}.")

    config = SHConfig()
    if profile == "sentinelhub":
        config.sh_client_id = cfg["sentinelhub"]["client_id"]
        config.sh_client_secret = cfg["sentinelhub"]["client_secret"]
    elif profile == "cdse":
        config.sh_client_id = cfg["cdse"]["client_id"]
        config.sh_client_secret = cfg["cdse"]["client_secret"]
        config.sh_token_url = cfg["cdse"]["token_url"]
        config.sh_base_url = cfg["cdse"]["base_url"]
    
    # config.save(profile)
    
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
        S2L2A_COLLECTION,
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
        data_collection=S2L2A_COLLECTION,
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

def get_bbox_tiles(aoi: Path, bbox: BBox) -> tuple[list[BBox], list[tuple[int, int]]]:
    """Splits a bounding box into smaller tiles if necessary, based on its size and the maximum tile size.

    Args:
        aoi (Path): Path to the GeoJSON file defining the area of interest
        bbox (BBox): Bounding box object defining the area of interest

    Returns:
        tuple[list[BBox], list[tuple[int, int]]]: Tuple containing a list of bounding box tiles and their corresponding sizes
    """
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
        
    return tiles, tile_sizes

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
    tiles, tile_sizes = get_bbox_tiles(aoi, bbox)
        
    nx, ny = get_number_of_tiles(bbox, resolution=RESOLUTION)
    bbox_size = bbox_to_dimensions(bbox, resolution=RESOLUTION)
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

def process_sentinel_responses(download_responses: list, search_results: CatalogSearchIterator, bbox: BBox) -> list[xr.DataArray]:
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
            raster_file = merge_tiles(response, "sentinel", bbox)
        else:
            raster_file = MemoryFile(response[0].content)
            
        with raster_file.open() as ds_rio:
            xarray = xr.open_dataarray(ds_rio, engine="rasterio").squeeze(drop=True)
            xarray = xarray.expand_dims("time")
            xarray = xarray.assign_coords(time=[np.datetime64(time[:-1])]) # numpy does not support timezone
            xarray.name = "NDVI"
            xarrays.append(xarray)
            
    return xarrays

def process_clms_responses(download_responses: list, dekadal_targets: list[str], bbox: BBox) -> list[xr.DataArray]:
    """Process raw CLMS responses into xarray DataArrays.

    Args:
        download_responses (list): List of CLMS download responses.
        dekadal_targets (list[str]): List of dekadal dates targetted in the data requests.
        bbox (BBox): Bounding box defining the area of interest.

    Returns:
        list[xr.DataArray]: List of xarray DataArrays containing the downloaded NDVI data.
    """
    
    xarrays = []
    for dekadal_target in dekadal_targets:
        
        # gets all tiles for this search target
        response = [resp for resp in download_responses
                    if json.loads(resp.request.body.decode('utf-8'))['input']['data'][0]['dataFilter']['timeRange']['from'][:10] == dekadal_target
                ]
        
        raster_file = None
        if len(response) > 1:
            raster_file = merge_tiles(response, "CLMS", bbox)
        else:
            raster_file = MemoryFile(response[0].content)
            
        with raster_file.open() as ds_rio:
            xarray = xr.open_dataarray(ds_rio, engine="rasterio").squeeze(drop=True)
            xarray = xarray.expand_dims("time")
            xarray = xarray.assign_coords(time=[np.datetime64(dekadal_target)]) # numpy does not support timezone
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
    xarrays = process_sentinel_responses(download_responses, search_results, bbox)
    
    return xarrays, target_times

def download_CLMS(aoi: Path, bbox: BBox, time_targets: list[str]) -> list[Response]:
    """Creates an OAuth2 session and downloads CLMS NDVI data for the specified dekadal target dates and area of interest.

    Args:
        aoi (Path): Path to the GeoJSON file defining the area of interest.
        bbox (BBox): Bounding box defining the area of interest.
        time_targets (list[str]): List of dekadal target dates.

    Raises:
        ValueError: If CLMS NDVI data is not available for the specified year.

    Returns:
        list[Response]: List of HTTP responses from the CLMS data requests.
    """
    
    config = setup_sh_config(profile="cdse")

    # Create a session
    client = BackendApplicationClient(client_id=config.sh_client_id)
    oauth = OAuth2Session(client=client)

    # Get token for the session
    token = oauth.fetch_token(token_url=config.sh_token_url, client_secret=config.sh_client_secret, include_client_id=True)
    
    tiles, tile_sizes = get_bbox_tiles(aoi, bbox)
    with open(ROOT_DIR / "src/clms_ndvi.js") as fp:
        evalscript = fp.read()
    
    responses = []
    for time_target in time_targets:
        byoc_collection = None
        
        target_year = int(time_target[:4])
        if target_year >= 2020:
            byoc_collection = CLMS_NDVI_V2
        elif target_year >= 2014 and target_year < 2020:
            byoc_collection = CLMS_NDVI_V1
        else:
            raise ValueError(f"CLMS NDVI data not available for year {target_year}. Supported years are from 2014 onwards.")
        
        glogger.info(f"Requesting CLMS data for dekadal target: {time_target}")
        for tile, tile_size in zip(tiles, tile_sizes):
            url, headers, body = get_CDSE_request(token, evalscript, byoc_collection, tile, (time_target, time_target))
            response = oauth.post(url, headers=headers, json=body)
            responses.append(response)
        
    return responses

def get_CDSE_request(token: dict, evalscript: str, collection_id: str, bbox: BBox, time_interval: tuple[str, str]) -> tuple[str, dict, dict]:
    """Builds a CDSE request for downloading data.

    Args:
        token (dict): OAuth2Session token
        evalscript (str): Evalscript for processing the data
        collection_id (str): CDSE collection ID for the data
        bbox (BBox): Bounding box defining the area of interest
        time_interval (tuple[str, str]): Time interval for the data request
    Returns:
        tuple[str, dict, dict]: URL, headers, and body for the CDSE request
    """
    
    bbox_size = bbox_to_dimensions(bbox, resolution=RESOLUTION)
           
    # request body/payload
    body_request = {
        'input': {
            'bounds': {
                'bbox': list(bbox),
                'properties': {
                    'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                }
            },
            'data': [
                {
                    'type': collection_id,
                    'dataFilter': {
                        'timeRange': {
                            'from': f'{time_interval[0]}T00:00:00Z',
                            'to': f'{time_interval[1]}T23:59:59Z'
                        }
                    },
                }
            ]
        },
        'output': {
            'width': bbox_size[0],
            'height': bbox_size[1],
            'responses': [
                {
                    'identifier': 'index',
                    'format': {
                        'type': 'image/tiff',
                    }
                }
            ]
        },
        'evalscript': evalscript
    }
    
    # Set the request URL and headers
    url_request = "https://sh.dataspace.copernicus.eu/api/v1/process"
    headers_request = {
        "Authorization": f"Bearer {token['access_token']}"
    }
    
    return url_request, headers_request, body_request
    