from pathlib import Path


def get_access_token() -> str:
    import os

    import requests

    r = requests.post(
        "https://auth.enterprise.wikimedia.com/v1/login",
        json={"username": os.getenv("USERNAME"), "password": os.getenv("PASSWORD")},
    )
    return r.json()["access_token"]


def get_snapshot_chunks(access_token: str, identifier: str) -> tuple[str, int]:
    import requests

    r = requests.get(
        f"https://api.enterprise.wikimedia.com/v2/snapshots/{identifier}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    data = r.json()
    return data["date_modified"].split("T")[0], len(data["chunks"])


def download_chunk(access_token: str, snapshot_id: str, chunk_id: str) -> Path:
    import requests

    r = requests.get(
        f"https://api.enterprise.wikimedia.com/v2/snapshots/{snapshot_id}/chunks/{chunk_id}/download",
        headers={"Authorization": f"Bearer {access_token}"},
        stream=True,
    )
    chunk_tar_path = get_chunk_tar_path(chunk_id)
    chunk_tar_path.parent.mkdir(exist_ok=True)
    with chunk_tar_path.open("wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    decompress_chunk(chunk_tar_path)
    chunk_tar_path.unlink()
    return get_chunk_ndjson_path(chunk_id)


def decompress_chunk(path: Path):
    import tarfile

    with tarfile.open(path) as tar:
        tar.extractall("build")


def get_chunk_tar_path(chunk_identifier: str) -> Path:
    return Path("build").joinpath(chunk_identifier).with_suffix(".tar.gz")


def get_chunk_ndjson_path(chunk_identifier: str) -> Path:
    filename = chunk_identifier[chunk_identifier.index("chunk_") :] + ".ndjson"
    return Path("build") / filename


def get_latest_release_date(identifier: str) -> str:
    import requests

    r = requests.get(
        f"https://github.com/xxyzz/snapshot/releases/latest/download/{identifier}.json"
    )
    if r.ok:
        return r.json()["date"]
    return ""


def is_newer_snapshot(current: str, last: str) -> bool:
    from datetime import datetime

    if last == "":
        return True
    current_date = datetime.fromisoformat(current)
    last_date = datetime.fromisoformat(last)
    return current_date > last_date


def edition_has_update(edition: str, access_token: str) -> bool:
    identifier = f"{edition}wiktionary_namespace_0"
    current_date, _ = get_snapshot_chunks(access_token, identifier)
    last_date = get_latest_release_date(identifier)
    return is_newer_snapshot(current_date, last_date)


def check_update(args):
    from .api import get_access_token
    from .edition import EDITIONS

    token = get_access_token()
    if any(edition_has_update(edition, token) for edition in EDITIONS.keys()):
        print("true")
    else:
        print("false")


def download_last_release(patterns: list[str]):
    from subprocess import run

    Path("build").mkdir(exist_ok=True)
    args = ["gh", "release", "download", "-D", "build"]
    for pattern in patterns:
        args.extend(["-p", pattern])
    run(args, check=True, capture_output=True, text=True)
