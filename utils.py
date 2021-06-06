import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.propegate = False
    log_stream_handler = logging.StreamHandler()
    log_fmt = '[%(asctime)s] <%(name)s>: %(message)s'
    log_dt_fmt = '%Y-%m-%d %H:%M:%S'
    log_formatter = logging.Formatter(log_fmt, datefmt=log_dt_fmt)
    log_stream_handler.setFormatter(log_formatter)
    logger.addHandler(log_stream_handler)
    logger.setLevel(logging.INFO)
    return logger
