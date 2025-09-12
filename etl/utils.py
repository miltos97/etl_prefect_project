import logging
import yaml

def setup_logger():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    logger = logging.getLogger("ETLLogger")
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if logger is reused
    if not logger.handlers:
        file_handler = logging.FileHandler(config["log_path"])
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

