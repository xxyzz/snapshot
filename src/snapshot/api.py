from pathlib import Path


def get_access_token() -> str:
    import os

    import requests

    r = requests.post(
        "https://auth.enterprise.wikimedia.com/v1/login",
        json={"username": os.getenv("USERNAME"), "password": os.getenv("PASSWORD")},
    )
    return r.json()["access_token"]


def get_snapshot_chunks(access_token: str, identifier: str) -> tuple[str, list[str]]:
    import requests

    r = requests.get(
        f"https://api.enterprise.wikimedia.com/v2/snapshots/{identifier}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    data = r.json()
    return data["date_modified"].split("T")[0], data["chunks"]


def download_chunk(
    access_token: str,
    snapshot_identifier: str,
    chunk_identifier: str,
) -> Path:
    import requests

    r = requests.get(
        f"https://api.enterprise.wikimedia.com/v2/snapshots/{snapshot_identifier}/chunks/{chunk_identifier}/download",
        headers={"Authorization": f"Bearer {access_token}"},
        stream=True,
    )
    chunk_tar_path = get_chunk_tar_path(chunk_identifier)
    chunk_tar_path.parent.mkdir(exist_ok=True)
    with chunk_tar_path.open("wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    decompress_chunk(chunk_tar_path)
    chunk_tar_path.unlink()
    return get_chunk_ndjson_path(chunk_identifier)


def decompress_chunk(path: Path):
    import tarfile

    with tarfile.open(path) as tar:
        tar.extractall("build")


def get_chunk_tar_path(chunk_identifier: str) -> Path:
    return Path("build").joinpath(chunk_identifier).with_suffix(".tar.gz")


def get_chunk_ndjson_path(chunk_identifier: str) -> Path:
    filename = chunk_identifier[chunk_identifier.index("chunk_") :] + ".ndjson"
    return Path("build") / filename
