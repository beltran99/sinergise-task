import logging

import xarray as xr
import numpy as np

def get_mvc(target: np.datetime64, masked_arr_l: xr.DataArray, masked_arr_r: xr.DataArray) -> xr.DataArray:
    """_summary_

    Args:
        target (np.datetime64): _description_
        masked_arr_l (xr.DataArray): _description_
        masked_arr_r (xr.DataArray): _description_

    Returns:
        xr.DataArray: _description_
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

def transform(ndvi_xarrays: list[xr.DataArray], target_times: np.ndarray, logger: logging.Logger) -> xr.DataArray:
    """_summary_

    Args:
        ndvi_xarrays (list[xr.DataArray]): _description_
        target_times (np.ndarray): _description_
        logger (logging.Logger): _description_

    Returns:
        xr.DataArray: _description_
    """

    # Combine all xarrays into a single one
    ndvi = xr.concat(ndvi_xarrays, dim="time")
    ndvi = ndvi.sortby("time")
    
    nan_count = np.count_nonzero(np.isnan(ndvi.values))
    arr_size = ndvi.values.size
    logger.info(f"Ratio of missing pixels: {nan_count/arr_size*100:.2f}%")
    logger.info(f"NDVI min value: {np.nanmin(ndvi.values)}, NDVI max value: {np.nanmax(ndvi.values)}")
    
    # since the linear interpolation only takes into account one point on each side,
    # we create a Maximum Value Composite around each dekadal target date
    
    logger.info("Creating Maximum Value Composite (MVC) for each dekadal period...")
    
    mvc = None
    for i, target in enumerate(target_times):
        
        masked_arr_l, masked_arr_r = None, None
        if i == 0: # first target
            masked_arr_l = ndvi.where(ndvi.time < target, drop=True)
            masked_arr_r = ndvi.where((ndvi.time > target) & (ndvi.time < target_times[i + 1]), drop=True)
            
        elif i == len(target_times) - 1: # last target
            masked_arr_l = ndvi.where((ndvi.time > target_times[i - 1]) & (ndvi.time < target), drop=True)
            masked_arr_r = ndvi.where(ndvi.time > target, drop=True)
            
        else: # middle targets
            masked_arr_l = ndvi.where((ndvi.time > target_times[i - 1]) & (ndvi.time < target), drop=True)
            masked_arr_r = ndvi.where((ndvi.time > target) & (ndvi.time < target_times[i + 1]), drop=True)
                
        logger.info(f"Using {[str(t.astype('datetime64[D]')) for t in masked_arr_l.time.values]} and {[str(t.astype('datetime64[D]')) for t in masked_arr_r.time.values]} to create MVC around target date {str(target.astype('datetime64[D]'))}...")
                
        mvc_partial = get_mvc(target, masked_arr_l, masked_arr_r)
        mvc = xr.concat([mvc, mvc_partial], dim="time") if mvc is not None else mvc_partial
        
    logger.info("Performing linear interpolation on MVC to obtain dekadal composite...")   
    interp_composite = mvc.interp(time=target_times, method="linear")   
    logger.info("Interpolated dekadal NDVI composite obtained successfully!")
    
    return interp_composite