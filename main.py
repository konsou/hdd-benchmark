import os.path
import random
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from time import sleep

LINES_TO_WRITE = 1000
CHARS_PER_LINE = 100


def main(paths: list[str] = None) -> None:
    if paths is None:
        paths = sys.argv[1:]
    print(f"Paths selected:")
    print('\n'.join(paths))

    if files_exist(paths=paths):
        print(f"FILES ALREADY EXIST - EXITING")
        sys.exit(1)

    print(f"Starting benchmark...")
    with ThreadPoolExecutor() as executor:
        for path in paths:
            executor.submit(write_random_data, path)
    print("Done.")

    print("Cleanup...")
    delete_files(paths=paths)
    print("Cleanup done.")


def files_exist(paths: list[str]) -> bool:
    found_existing = False
    for path in paths:
        if os.path.exists(path):
            print(f"{path} exists")
            found_existing = True
    return found_existing


def delete_files(paths: list[str]) -> None:
    for path in paths:
        print(f"Deleting {path}")
        os.remove(path)


def write_random_data(path: str) -> None:
    print(f"Writing to {path}, {LINES_TO_WRITE} open/close(s), {CHARS_PER_LINE} chars per line")
    for _ in range(LINES_TO_WRITE):
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f"{random_string(length=CHARS_PER_LINE)}\n")


def random_string(length: int) -> str:
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


if __name__ == '__main__':
    main(paths=[
        "asdf.txt",
        "moimoim.txt",
        "jeejee.txt",
    ])
