import argparse
import logging
from pathlib import Path
from sentinelhub import BBox, CRS

from src.utils import geojson_to_bbox
from src.logger import get_logger
from src.extract import extract
from src.transform import transform
from src.load import load

ROOT_DIR = Path(__file__).parent.parent.resolve()

def pipeline(bbox: BBox, time_interval: tuple[str, str], aoi: Path, logger: logging.Logger) -> None:
    """_summary_

    Args:
        aoi (str): _description_
        bbox (BBox): _description_
        time_interval (tuple[str, str]): _description_
        logger (logging.Logger): _description_
    """
    
    xarrays, target_times = extract(bbox, time_interval, aoi, logger)
    composite = transform(xarrays, target_times, logger)
    load(composite, logger)


if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser(description="Download Sentinel-2 data from Sentinel Hub.")
    argparser.add_argument("--aoi", type=str, help="Path to GeoJSON file defining the area of interest", default="data/input/AOI_for_test.geojson")
    argparser.add_argument("--start-date", type=str, help="Start date in YYYY-MM-DD format", default="2025-08-01")
    argparser.add_argument("--end-date", type=str, help="End date in YYYY-MM-DD format", default="2025-08-31")
    argparser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output", default=False)
    args = argparser.parse_args()
    
    aoi_path = ROOT_DIR / args.aoi
    bbox = geojson_to_bbox(aoi_path)
    bbox = BBox(bbox=bbox, crs=CRS.WGS84)
    time_interval = (args.start_date, args.end_date)
    
    logging_level = logging.INFO
    if not args.verbose:
        logging_level = logging.ERROR
    logger = get_logger(__file__, level=logging_level)
    
    pipeline(bbox, time_interval, aoi_path, logger)