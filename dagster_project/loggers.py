import logging
from dagster import logger, Field

@logger(
    config_schema={
        "name": Field(str, is_required=False, default_value="my_logger"),
        "log_level": Field(str, is_required=False, default_value="INFO")
    }
)

def log_status(init_context):
    log_level = init_context.logger_config["log_level"]
    name = init_context.logger_config["name"]

    logger_ = logging.getLogger(name)
    logger_.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger_.addHandler(handler)
    return logger_
