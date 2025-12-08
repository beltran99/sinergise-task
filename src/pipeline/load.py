import logging
import matplotlib.pyplot as plt
from pathlib import Path

import xarray as xr

ROOT_DIR = Path(__file__).parent.parent.parent.resolve()

def plot_interp_composite(interp_composite: xr.DataArray) -> str:
    """Generates and saves a plot of the interpolated NDVI composite time series.

    Args:
        interp_composite (xr.DataArray): Interpolated NDVI composite time series
        
    Returns:
        str: Path to the saved plot.
    """
    fig, axes = plt.subplots(
        nrows=(len(interp_composite.time) + 2) // 3, 
        ncols=3, 
        figsize=(24, 3 * ((len(interp_composite.time) + 2) // 3))
    )

    for i, t in enumerate(interp_composite.time.values):
        j = i // 3
        ax = axes[j][i % 3] if len(interp_composite.time) > 3 else axes[i % 3]
        interp_composite.sel(time=t).plot.imshow(ax=ax, cmap='PRGn')
        ax.set_title(str(t.astype('datetime64[D]')))
        ax.set_xlabel("")
        ax.set_ylabel("")

    plt.tight_layout()
    out_path = ROOT_DIR / "data/output/ndvi_composite.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    
    return str(out_path)
    
def load(interp_composite: xr.DataArray, logger: logging.Logger) -> None:
    """Loads the interpolated composite time series to disk in NetCDF and GeoTIFF formats, and generates plot.

    Args:
        interp_composite (xr.DataArray): Interpolated NDVI composite time series.
        logger (logging.Logger): Logger object for logging information.
    """
    
    logger.info("Saving interpolated composite to disk...")
    
    out_path = ROOT_DIR / "data/output/ndvi_composite.nc"
    interp_composite.to_netcdf(out_path)
    logger.info(f"Saved interpolated NDVI composite time series in NetCDF format to {out_path}")
    
    out_path = ROOT_DIR / "data/output/ndvi_composite.tif"
    interp_composite.rio.to_raster(out_path)
    logger.info(f"Saved interpolated NDVI composite time series in GeoTIFF format to {out_path}")
    
    out_path = plot_interp_composite(interp_composite)
    logger.info(f"Saved interpolated NDVI composite time series plot to {out_path}")