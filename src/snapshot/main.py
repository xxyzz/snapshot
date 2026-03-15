import logging

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def worker(access_token: str, identifier: str, chunk_num: int):
    from .api import download_chunk
    from .compress import compress_chunk

    chunk = f"{identifier}_chunk_{chunk_num}"
    logger.info(f"Downloading {chunk}")
    ndjson_path = download_chunk(access_token, identifier, chunk)
    compress_chunk(chunk, ndjson_path)


def create_files(edition: str, ns_id: int, access_token: str):
    from concurrent.futures import ProcessPoolExecutor
    from functools import partial

    from .api import download_last_release, get_snapshot_chunks
    from .compress import create_json, get_latest_release_date, is_newer_snapshot

    identifier = f"{edition}wiktionary_namespace_{ns_id}"
    last_date = get_latest_release_date(identifier)
    current_date, chunks = get_snapshot_chunks(access_token, identifier)
    if is_newer_snapshot(current_date, last_date):
        create_json(identifier, current_date, chunks)
        with ProcessPoolExecutor() as executor:
            executor.map(partial(worker, access_token, identifier), range(chunks))
    else:
        download_last_release(edition, ns_id)


def build(args):
    from .api import get_access_token
    from .namespace import NAMESPACES

    access_token = get_access_token()
    for ns_id in NAMESPACES[args.edition]:
        create_files(args.edition, ns_id, access_token)


def main():
    import argparse

    from .api import check_update
    from .namespace import NAMESPACES

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    check_parser = subparsers.add_parser("check")
    check_parser.set_defaults(func=check_update)
    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("edition", choices=NAMESPACES.keys())
    build_parser.set_defaults(func=build)
    args = parser.parse_args()
    args.func(args)
