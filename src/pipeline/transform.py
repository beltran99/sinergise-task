import logging
from pathlib import Path

import xarray as xr
import numpy as np
from sentinelhub import BBox

from src.pipeline.extract import download_CLMS, process_clms_responses

glogger = None

def get_mvc(target: np.datetime64, masked_arr_l: xr.DataArray, masked_arr_r: xr.DataArray) -> xr.DataArray:
    """Computes Maximum Value Composites (MVC) around a target date. Given two time series of NDVI values
    before and after the target date, it selects the maximum NDVI values from each side and retrieves
    a new composite DataArray with both values.

    Args:
        target (np.datetime64): Target date around which the MVCs are computed
        masked_arr_l (xr.DataArray): Time series of NDVI values before the target date
        masked_arr_r (xr.DataArray): Time series of NDVI values after the target date

    Returns:
        xr.DataArray: MVC DataArray around the target date
    """
    if len(masked_arr_l.time.values) > 0:
        masked_arr_l = masked_arr_l.max(dim="time", skipna=True)
        masked_arr_l = masked_arr_l.expand_dims("time")
        masked_arr_l = masked_arr_l.assign_coords(time=[target - np.timedelta64(1,'D')])
    if len(masked_arr_r.time.values) > 0:
        masked_arr_r = masked_arr_r.max(dim="time", skipna=True)
        masked_arr_r = masked_arr_r.expand_dims("time")
        masked_arr_r = masked_arr_r.assign_coords(time=[target + np.timedelta64(1,'D')])
    
    return xr.concat([masked_arr_l, masked_arr_r], dim="time")

def transform(ndvi_xarrays: list[xr.DataArray], target_times: np.ndarray, aoi: Path, bbox: BBox, logger: logging.Logger) -> xr.DataArray:
    """Transforms the raw downloaded NDVI data into a dekadal composite time series.

    Args:
        ndvi_xarrays (list[xr.DataArray]): List of NDVI xarray DataArrays
        target_times (np.ndarray): Target dekadal dates for the composite
        aoi (Path): Path to the GeoJSON file defining the area of interest
        bbox (BBox): Bounding box object defining the area of interest
        logger (logging.Logger): Logger object for logging information

    Returns:
        xr.DataArray: Dekadal composite NDVI time series
    """
    global glogger
    glogger = logger

    # Combine all xarrays into a single one
    ndvi = xr.concat(ndvi_xarrays, dim="time")
    ndvi = ndvi.sortby("time")
    
    nan_count = np.count_nonzero(np.isnan(ndvi.values))
    arr_size = ndvi.values.size
    logger.info(f"Ratio of missing pixels: {(nan_count/arr_size)*100:.2f}%")
    logger.info(f"NDVI min value: {np.nanmin(ndvi.values)}, NDVI max value: {np.nanmax(ndvi.values)}")
    
    # since the linear interpolation only takes into account one point on each side,
    # we create a Maximum Value Composite around each dekadal target date
    
    logger.info("Creating Maximum Value Composite (MVC) for each dekadal period...")
    
    mvc = None
    for i, target in enumerate(target_times):
        
        masked_arr_l = ndvi.where((ndvi.time < target) & (ndvi.time > target - np.timedelta64(10,'D')), drop=True)
        masked_arr_r = ndvi.where((ndvi.time > target) & (ndvi.time < target + np.timedelta64(10,'D')), drop=True)
                
        logger.info(f"Using {[str(t.astype('datetime64[D]')) for t in masked_arr_l.time.values]} and {[str(t.astype('datetime64[D]')) for t in masked_arr_r.time.values]} to create MVC around target date {str(target.astype('datetime64[D]'))}...")
                
        mvc_partial = get_mvc(target, masked_arr_l, masked_arr_r)
        mvc = xr.concat([mvc, mvc_partial], dim="time") if mvc is not None else mvc_partial
        
    logger.info("Performing linear interpolation on MVCs to obtain dekadal composite...")   
    interp_composite = mvc.interp(time=target_times, method="linear")
    logger.info("Interpolated dekadal NDVI composite obtained successfully!")
    
    # Check for gaps in the dekadal composite and
    # fill them using lower-resolution CLMS product
    
    gaps = []
    for t in interp_composite.time.values:
        if np.all(np.isnan(interp_composite.sel(time=t).values)):
            logger.info(f"Gap detected for dekadal {str(t.astype('datetime64[D]'))}. Will attempt to fill using CLMS data.")
            gaps.append(t)
            
    if gaps:
        interp_composite = fill_gaps(gaps, interp_composite, aoi, bbox)
    
    return interp_composite

def fill_gaps(gaps: list[np.datetime64], composite: xr.DataArray, aoi: Path, bbox: BBox) -> xr.DataArray:
    """Fills gaps in the Sentinel-2 dekadal composite using lower-resolution CLMS data.

    Args:
        gaps (list[np.datetime64]): List of dekadal dates with gaps to be filled
        composite (xr.DataArray): Sentinel-2 dekadal composite NDVI time series
        aoi (Path): Path to the GeoJSON file defining the area of interest
        bbox (BBox): Bounding box defining the area of interest

    Returns:
        xr.DataArray: Dekadal composite NDVI time series with gaps filled using CLMS data
    """
    
    target_dekadals = [str(t.astype('datetime64[D]')) for t in gaps]
    responses = download_CLMS(aoi, bbox, target_dekadals)
    xarrays = process_clms_responses(responses, target_dekadals, bbox)
    clms_xarray = xr.concat(xarrays, dim="time")
    
    factor = 1 / 250.0
    offset = -0.08
    for gap in gaps:
        glogger.info(f"Filling gap for dekadal {str(gap.astype('datetime64[D]'))} using CLMS data...")
        clms_data = clms_xarray.sel(time=gap, band=2) * factor + offset
        composite.loc[dict(time=gap)] = clms_data
        
    return composite