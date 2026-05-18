from pathlib import Path
from sqlite3 import Connection


def download_title_sql_dumps(edition: str) -> list[Path]:
    paths = []
    for file in ("-page.sql.gz", "-redirect.sql.gz"):
        paths.append(
            download_title_sql_dump(
                f"https://dumps.wikimedia.org/{edition}wiktionary/latest/{edition}wiktionary-latest{file}"
            )
        )
    return paths


def download_title_sql_dump(url: str) -> Path:
    import gzip
    import shutil
    from importlib.metadata import version

    import requests

    from .main import logger

    filename = url.rsplit("/", maxsplit=1)[-1]
    sql_gz_path = Path("build") / filename
    sql_path = sql_gz_path.with_name(sql_gz_path.stem)
    if not sql_path.exists() and not sql_gz_path.exists():
        logger.info(f"Downloading {filename}")
        r = requests.get(
            url,
            headers={
                "user-agent": f"snapshot/{version('snapshot')} (https://github.com/xxyzz/snapshot)"
            },
            stream=True,
        )
        with sql_gz_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"{filename} downloaded")
    if not sql_path.exists():
        with gzip.open(sql_gz_path, "rb") as f_in, sql_path.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        sql_gz_path.unlink()
    return sql_path


def init_redirect_db(edition: str) -> tuple[Path, Connection]:
    import sqlite3

    db_path = Path(f"build/{edition}_redirect.db")
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE redirect (
      source   TEXT PRIMARY KEY,
      target   TEXT,
      fragment TEXT
    );
    """)

    return db_path, conn


def parse_sql_line(line: str):
    import csv
    import io
    import re

    line = re.sub(r"^INSERT INTO `.+` VALUES \(", "", line)
    line = line.strip("(); \n").replace("),(", "\n")
    return csv.reader(
        io.StringIO(line),
        delimiter=",",
        quotechar="'",
        escapechar="\\",
        doublequote=False,
    )


def parse_page_sql(sql_path: Path) -> dict[str, str]:
    pages = {}
    with sql_path.open() as f:
        for line in f:
            if line.startswith("INSERT INTO "):
                for row in parse_sql_line(line):
                    page_id, namespace, title, is_redirect, *_ = row
                    if namespace != "0" or is_redirect != "1":
                        continue
                    pages[page_id] = title.replace("_", " ")
    return pages


def parse_redirect_sql(sql_path: Path, pages: dict[str, str], conn: Connection):
    with sql_path.open() as f:
        for line in f:
            if line.startswith("INSERT INTO "):
                for row in parse_sql_line(line):
                    from_id, namespace, title, interwiki, fragment = row
                    if namespace != "0" or interwiki != "" or from_id not in pages:
                        continue
                    title = title.replace("_", " ")
                    conn.execute(
                        "INSERT INTO redirect VALUES(?, ?, ?)",
                        (pages.get(from_id, ""), title, fragment.replace(" ", "_")),
                    )


def create_redirect_db(edition: str):
    import shutil
    from compression import zstd

    from .main import logger

    input_sql_paths = download_title_sql_dumps(edition)
    db_path, conn = init_redirect_db(edition)
    pages = {}
    for input_path in input_sql_paths:
        if input_path.name.endswith("-page.sql"):
            pages = parse_page_sql(input_path)
        else:
            parse_redirect_sql(input_path, pages, conn)
        input_path.unlink()
    conn.executescript("""
    CREATE INDEX target_idx ON redirect (target);
    PRAGMA optimize;
    """)
    conn.commit()
    conn.close()
    zst_path = db_path.with_suffix(db_path.suffix + ".zst")
    with db_path.open("rb") as f_in, zstd.open(zst_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    db_path.unlink()
    logger.info("Redirect db created")
