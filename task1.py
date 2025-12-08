import argparse
import logging
from pathlib import Path
from sentinelhub import BBox, CRS

from src.utils import geojson_to_bbox
from src.logger import get_logger
from src.pipeline.extract import extract
from src.pipeline.transform import transform
from src.pipeline.load import load

ROOT_DIR = Path(__file__).parent.resolve()

def execute_pipeline(bbox: BBox, time_interval: tuple[str, str], aoi: Path, logger: logging.Logger) -> None:
    """Builds and executes the ETL pipeline for Sentinel-2 data.

    Args:
        bbox (BBox): Bounding box object defining the area of interest.
        time_interval (tuple[str, str]): Time interval for time series generation in (start, end) format.
        aoi (Path): Path to the GeoJSON file defining the area of interest.
        logger (logging.Logger): Logger object for logging messages.
    """
    # 1. Extract data through Sentinel Hub API according to defined criteria
    xarrays, target_times = extract(bbox, time_interval, aoi, logger)
    # 2. Transform the extracted data into a composite time series
    composite = transform(xarrays, target_times, logger)
    # 3. Save the output composite time series to the desired destination and format
    load(composite, logger)


if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser(description="Download Sentinel-2 data from Sentinel Hub.")
    argparser.add_argument("--aoi", type=str, help="Path to GeoJSON file defining the area of interest. Please define path from project root.", default="data/input/AOI_for_test.geojson")
    argparser.add_argument("--start", type=str, help="Start date in YYYY-MM-DD format", default="2025-08-01")
    argparser.add_argument("--end", type=str, help="End date in YYYY-MM-DD format", default="2025-08-31")
    argparser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output", default=False)
    args = argparser.parse_args()
    
    # Parse CLI arguments
    aoi_path = ROOT_DIR / args.aoi
    bbox = geojson_to_bbox(aoi_path)
    bbox = BBox(bbox=bbox, crs=CRS.WGS84)
    time_interval = (args.start_date, args.end_date)
    
    # Set up logger
    logging_level = logging.INFO
    if not args.verbose:
        logging_level = logging.ERROR
    logger = get_logger(name='ETL Pipeline', level=logging_level)
    
    execute_pipeline(bbox, time_interval, aoi_path, logger)