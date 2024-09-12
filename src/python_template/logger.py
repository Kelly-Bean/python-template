import logging
import logging.handlers


def get_logger(name, log_level="INFO"):
    logger = logging.getLogger(name)
    logger.propagate = False

    ch = logging.StreamHandler()
    logger.setLevel(log_level)
    ch.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(module)s:%(funcName)s:%(lineno)d] : %(message)s")
    ch.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(ch)

    return logger

