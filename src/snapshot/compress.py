import json
from pathlib import Path


def get_latest_release_date(identifier: str) -> str:
    import requests

    r = requests.get(
        f"https://github.com/xxyzz/snapshot/latest/releases/{identifier}.json"
    )
    if r.ok:
        return r.json()["date"]
    return ""


def is_newer_snapshot(current: str, last: str) -> bool:
    from datetime import datetime

    if last == "":
        return True
    current_date = datetime.utcfromtimestamp(current)
    last_date = datetime.utcfromtimestamp(last)
    return current_date > last_date


def compress_chunk(chunk_identifier: str, ndjson_path: Path):
    import shutil
    from compression import zstd

    from .main import logger

    new_ndjson_path = Path(f"build/{chunk_identifier}.ndjson")
    with ndjson_path.open() as f_in, new_ndjson_path.open("w") as f_out:
        for line in f_in:
            data = json.loads(line)
            json.dump(
                {"name": data["name"], "html": data["article_body"]["html"]},
                f_out,
                ensure_ascii=False,
            )
            f_out.write("\n")
    logger.info(f"{chunk_identifier} filter done")
    zst_path = new_ndjson_path.with_suffix(".zst")
    with new_ndjson_path.open("rb") as f_in, zstd.open(zst_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    ndjson_path.unlink()
    new_ndjson_path.unlink()
    logger.info(f"{chunk_identifier} compress done")


def create_json(identifier: str, date: str, chunks: list[str]):
    with open(f"build/{identifier}.json", "w") as f:
        json.dump({"date": date, "chunks": chunks}, f)
