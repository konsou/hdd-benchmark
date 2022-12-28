#!/usr/bin/env python3
import os.path
import random
import string
import sys
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from typing import List

from logger import init_logger

LINES_TO_WRITE = 1000
CHARS_PER_LINE = 100
LOGFILE = 'benchmark_log.txt'

logger = init_logger(logfile=LOGFILE)


def main(paths: List[str] = None) -> None:
    logger.info("----------------NEW RUN----------------")
    logger.info(f"Logfile is {LOGFILE}")
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


def write_random_data(path: str) -> None:
    logger.info(f"Writing to {path}, {LINES_TO_WRITE} open/close(s), {CHARS_PER_LINE} chars per line")
    start_time = perf_counter()
    for _ in range(LINES_TO_WRITE):
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f"{random_string(length=CHARS_PER_LINE)}\n")
    logger.info(f"{path} writes finished in {perf_counter() - start_time} seconds")


def random_string(length: int) -> str:
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


if __name__ == '__main__':
    main()
