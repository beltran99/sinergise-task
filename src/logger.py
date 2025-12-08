import logging

def get_logger(name: str = __name__, level=logging.INFO) -> logging.Logger:
    """Create or get a logger configured with consistent formatting."""
    logger = logging.getLogger(name)
    if not logger.handlers: # avoid duplicates
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%d/%m/%Y %I:%M:%S %p',
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    
    return logger