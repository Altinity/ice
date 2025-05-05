import httpx
import os
import hashlib
import sys


def download(url: str) -> str:
    filename = get_hashed_filename(url)
    if not os.path.exists(filename):
        print(f"Downloading {url} as {filename}...", file=sys.stderr)
        with httpx.stream("GET", url) as r:
            r.raise_for_status()
            with open(filename + "~", "wb") as w:
                for chunk in r.iter_bytes():
                    w.write(chunk)
            os.rename(filename + "~", filename)
    return filename


def get_hashed_filename(url: str) -> str:
    hash_object = hashlib.sha256(url.encode("utf-8"))
    return hash_object.hexdigest()[:16] + ".cached"
