import logging
import matplotlib.pyplot as plt
from pathlib import Path

import xarray as xr

ROOT_DIR = Path(__file__).parent.parent.resolve()

def plot_interp_composite(interp_composite: xr.DataArray) -> None:
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
    plt.savefig(ROOT_DIR / "data/output/ndvi_plot.png", dpi=150, bbox_inches="tight")
    plt.close()
    
def load(interp_composite: xr.DataArray, logger: logging.Logger) -> None:
    interp_composite.to_netcdf(ROOT_DIR / "data/output/ndvi_composite.nc")
    interp_composite.rio.to_raster(ROOT_DIR / "data/output/ndvi_composite.tif")
    plot_interp_composite(interp_composite)