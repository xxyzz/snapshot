def compress_parsoid_chunk(access_token: str, identifier: str, chunk_num: int):
    import json
    import shutil
    from compression import zstd
    from pathlib import Path

    from .api import download_chunk
    from .main import logger

    chunk = f"{identifier}_chunk_{chunk_num}"
    logger.info(f"Downloading {chunk}")
    ndjson_path = download_chunk(access_token, identifier, chunk)
    logger.info(f"Start {chunk}")
    new_ndjson_path = Path(f"build/{chunk}.ndjson")
    with ndjson_path.open() as f_in, new_ndjson_path.open("w") as f_out:
        for line in f_in:
            data = json.loads(line)
            json.dump(
                {"name": data["name"], "html": data["article_body"]["html"]},
                f_out,
                ensure_ascii=False,
            )
            f_out.write("\n")
    logger.info(f"{chunk} filter done")
    zst_path = new_ndjson_path.with_suffix(".zst")
    with new_ndjson_path.open("rb") as f_in, zstd.open(zst_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    ndjson_path.unlink()
    new_ndjson_path.unlink()
    logger.info(f"{chunk} compress done")


def create_parsoid_files(edition: str, ns_id: int, access_token: str):
    import json
    from concurrent.futures import ProcessPoolExecutor
    from functools import partial
    from os import process_cpu_count

    from .api import get_snapshot_chunks

    identifier = f"{edition}wiktionary_namespace_{ns_id}"
    snapshot_date, chunks, _ = get_snapshot_chunks(access_token, identifier)
    with open(f"build/{identifier}.json", "w") as f:
        json.dump({"date": snapshot_date, "chunks": chunks}, f)
    with ProcessPoolExecutor(max_workers=min(chunks, process_cpu_count())) as executor:
        executor.map(
            partial(compress_parsoid_chunk, access_token, identifier), range(chunks)
        )
