import logging
import sys


def init_logger(level=logging.INFO,
                logfile='benchmark_log.txt') -> logging.getLoggerClass():
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s',
                                  '%m-%d-%Y %H:%M:%S')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(logfile)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)

    return logger
