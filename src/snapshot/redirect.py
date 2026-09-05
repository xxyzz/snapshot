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
                "user-agent": f"snapshot/{version('snap')} (https://github.com/xxyzz/snapshot)"
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


def create_redirect_db(edition: str):
    # https://www.mediawiki.org/wiki/Manual:Redirect_table
    # https://www.mediawiki.org/wiki/Manual:Page_table
    import shutil
    import subprocess
    from compression import zstd

    import mariadb

    from .main import logger

    db_path, sqlite_conn = init_redirect_db(edition)
    dump_sql_paths = download_title_sql_dumps(edition)

    for sql_path in dump_sql_paths:
        with open(sql_path) as f:
            subprocess.run(
                [
                    "mariadb",
                    "--host=127.0.0.1",
                    "--port=3306",
                    "--user=root",
                    "--password=password",
                    "--database=main",
                    "--silent",
                ],
                check=True,
                stdin=f,
            )
        sql_path.unlink()
    with mariadb.connect("mariadb://root:password@127.0.0.1:3306/main") as mariadb_conn:
        with mariadb_conn.cursor() as cursor:
            cursor.execute("""
            SELECT page_title, rd_title, rd_fragment
            FROM page INNER JOIN redirect ON page.page_id = redirect.rd_from
            WHERE rd_namespace = 0 AND page_namespace = 0 AND rd_interwiki = ''
            """)
            for page_title, rd_title, rd_fragment in cursor:
                sqlite_conn.execute(
                    "INSERT INTO redirect VALUES(?, ?, ?)",
                    (
                        page_title.decode("utf-8").replace("_", " "),
                        rd_title.decode("utf-8").replace("_", " "),
                        rd_fragment.decode("utf-8").replace(" ", "_"),
                    ),
                )

    sqlite_conn.executescript("""
    CREATE INDEX target_idx ON redirect (target);
    PRAGMA optimize;
    """)
    sqlite_conn.commit()
    sqlite_conn.close()
    zst_path = db_path.with_suffix(db_path.suffix + ".zst")
    with db_path.open("rb") as f_in, zstd.open(zst_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    db_path.unlink()
    logger.info("Redirect db created")
