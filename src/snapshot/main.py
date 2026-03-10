import logging

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def worker(access_token: str, identifier: str, chunk: str):
    from .api import download_chunk
    from .compress import compress_chunk

    logger.info(f"Downloading {chunk}")
    ndjson_path = download_chunk(access_token, identifier, chunk)
    compress_chunk(chunk, ndjson_path)


def create_files(identifier: str, access_token: str):
    from concurrent.futures import ProcessPoolExecutor
    from functools import partial

    from .api import get_snapshot_chunks
    from .compress import create_json, get_latest_release_date, is_newer_snapshot

    last_date = get_latest_release_date(identifier)
    current_date, chunks = get_snapshot_chunks(access_token, identifier)
    if is_newer_snapshot(current_date, last_date):
        create_json(identifier, current_date, chunks)
        with ProcessPoolExecutor() as executor:
            executor.map(partial(worker, access_token, identifier), chunks)


def main():
    import argparse

    from .api import get_access_token
    from .namespace import NAMESPACES

    parser = argparse.ArgumentParser()
    parser.add_argument("edition", choices=NAMESPACES.keys())
    args = parser.parse_args()

    access_token = get_access_token()
    for ns_id in NAMESPACES[args.edition]:
        create_files(f"{args.edition}wiktionary_namespace_{ns_id}", access_token)
