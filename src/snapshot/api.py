from pathlib import Path


def get_access_token(check_token: bool) -> str:
    import os

    import requests

    token = os.getenv("ACCESSTOKEN")
    if token is None:
        token = get_access_token_from_api()
    elif check_token:
        r = requests.get(
            "https://api.enterprise.wikimedia.com/v2/snapshots/enwiktionary_namespace_10",
            headers={"Authorization": f"Bearer {token}"},
        )
        if not r.ok:
            if r.text == "Jwt is expired":
                token = refresh_token(os.getenv("REFRESHTOKEN"))
            else:
                raise Exception(
                    f"Invalid token: {r.status_code=} {r.reason=} {r.text=}"
                )
    return token


def get_access_token_from_api() -> str:
    import os
    from subprocess import run

    import requests

    r = requests.post(
        "https://auth.enterprise.wikimedia.com/v1/login",
        json={"username": os.getenv("USERNAME"), "password": os.getenv("PASSWORD")},
    )
    if r.ok:
        data = r.json()
        access_token = data["access_token"]
        run(["gh", "secret", "set", "ACCESSTOKEN", "-b", access_token], check=True)
        run(
            ["gh", "secret", "set", "REFRESHTOKEN", "-b", data["refresh_token"]],
            check=True,
        )
        return access_token
    else:
        raise Exception(f"Get token failed: {r.status_code=} {r.reason=} {r.text=}")


def refresh_token(refresh_t: str) -> str:
    import os
    from subprocess import run

    import requests

    from .main import logger

    r = requests.post(
        "https://auth.enterprise.wikimedia.com/v1/token-refresh",
        json={
            "username": os.getenv("USERNAME"),
            "refresh_token": os.getenv("REFRESHTOKEN"),
        },
    )
    if r.ok:
        token = r.json()["access_token"]
        run(["gh", "secret", "set", "ACCESSTOKEN", "-b", token], check=True)
        return token
    else:
        logger.warning(f"Refresh token failed: {r.status_code=} {r.reason=} {r.text=}")
        return get_access_token_from_api()


def get_snapshot_info(access_token: str, identifier: str) -> dict:
    import json

    import requests

    json_path = Path("build/info.json")
    if not json_path.exists():
        r = requests.get(
            "https://api.enterprise.wikimedia.com/v2/snapshots",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if r.ok:
            api_data = r.json()
            all_data = {}
            for data in api_data:
                all_data[data["identifier"]] = {
                    "date": data["date_modified"].split("T")[0],
                    "size": data["size"]["value"],
                    "chunks": len(data["chunks"]),
                }
            with json_path.open("w") as f:
                json.dump(all_data, f)
            return all_data[identifier]
        else:
            raise Exception(f"Get info failed: {r.status_code=} {r.reason=} {r.text=}")
    else:
        with json_path.open() as f:
            return json.load(f)[identifier]


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


def get_latest_release_data(identifier: str) -> dict:
    import requests

    r = requests.get(
        f"https://github.com/xxyzz/snapshot/releases/latest/download/{identifier}.json"
    )
    if r.ok:
        return r.json()
    return {}


def is_newer_snapshot(current: str, last: str) -> bool:
    from datetime import datetime

    if last == "":
        return True
    current_date = datetime.fromisoformat(current)
    last_date = datetime.fromisoformat(last)
    return current_date > last_date


def edition_has_update(edition: str, access_token: str) -> bool:
    from .main import logger

    identifier = f"{edition}wiktionary_namespace_0"
    snapshot_info = get_snapshot_info(access_token, identifier)
    current_date = snapshot_info["date"]
    current_chunks = snapshot_info["chunks"]
    current_size = snapshot_info["size"]
    last_release = get_latest_release_data(identifier)
    last_date = last_release.get("date", "")
    has_update = is_newer_snapshot(current_date, last_date)
    if has_update:
        last_chunks = last_release.get("chunks", 0)
        if current_chunks < last_chunks:
            logger.info(
                f"{edition} edition chunks decrease: {last_date} has {last_chunks} "
                f"chunks, {current_date} has {current_chunks} chunks"
            )
        last_size = last_release.get("size", 0)
        if current_size < last_size:
            logger.info(
                f"{edition} edition size decrease: {last_date} is {last_size}MB, "
                f"{current_date} is {current_size}MB"
            )
        if current_chunks < last_chunks and current_size < last_size:
            has_update = False
    return has_update


def check_update(args):
    from .edition import EDITIONS

    token = get_access_token(True)
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
    run(args, check=True)
