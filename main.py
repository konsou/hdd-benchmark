#!/usr/bin/env python3
import logging
import os.path
import random
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from typing import List

from logger import init_logger

LINES_TO_WRITE = 10_000
CHARS_PER_LINE = 270
READ_DELAY = 0.1
LOGFILE = 'benchmark_log.txt'

logger = init_logger(logfile=LOGFILE, level=logging.INFO)


def main(paths: List[str] = None) -> None:
    logger.info("----------------NEW RUN----------------")
    logger.info(f"Logfile is {LOGFILE}")
    logger.info(f"LINES_TO_WRITE: {LINES_TO_WRITE}")
    logger.info(f"CHARS_PER_LINE: {CHARS_PER_LINE}")
    logger.info(f"READ_DELAY: {READ_DELAY}")

    if paths is None:
        paths = sys.argv[1:]

    if not paths:
        logger.error(f"No paths set. Exiting.")
        sys.exit(1)

    paths_string = ', '.join(paths)
    logger.info(f"Paths selected: {paths_string}")

    if files_exist(paths=paths):
        logger.info(f"FILES ALREADY EXIST - EXITING")
        sys.exit(1)

    logger.info(f"Starting benchmark...")
    start_time = perf_counter()

    create_files(paths=paths)

    with ThreadPoolExecutor() as executor:
        for path in paths:
            executor.submit(write_random_data, path)
    logger.info("Done.")

    logger.info("Cleanup...")
    delete_files(paths=paths)
    logger.info("Cleanup done.")

    elapsed = perf_counter() - start_time
    logger.info(f"Benchmark took {elapsed} seconds.")


def files_exist(paths: List[str]) -> bool:
    found_existing = False
    for path in paths:
        if os.path.exists(path):
            logger.info(f"{path} exists")
            found_existing = True
    return found_existing


def delete_files(paths: List[str]) -> None:
    for path in paths:
        logger.info(f"Deleting {path}")
        os.remove(path)


def create_files(paths: List[str]) -> None:
    for path in paths:
        with open(path, 'a') as f:
            logger.info(f"Creating file {path}")


def write_random_data(path: str) -> None:
    logger.info(f"Writing to {path}, {LINES_TO_WRITE} open/close(s), {CHARS_PER_LINE} chars per line")
    start_time = perf_counter()
    stop_event = threading.Event()
    read_thread = threading.Thread(target=read_file_continuously, args=(path, stop_event, READ_DELAY))
    read_thread.start()
    for _ in range(LINES_TO_WRITE):
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f"{random_string(length=CHARS_PER_LINE)}\n")
    stop_event.set()
    logger.info(f"{path} writes finished in {perf_counter() - start_time} seconds")


def read_file_continuously(path: str,
                           stop_event: threading.Event,
                           delay_between_reads: int) -> None:
    logger.info(f"Starting read thread for {path}")
    while not stop_event.is_set():
        with open(path, encoding='utf-8') as f:
            logger.debug(f"Reading file {path}")
            f.read()
        time.sleep(delay_between_reads)
    logger.info(f"Stopping read thread for {path}")


def random_string(length: int) -> str:
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


if __name__ == '__main__':
    main()
