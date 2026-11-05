from pathlib import Path
from sqlite3 import Connection


def download_redirect_sql_dump(edition: str) -> Path:
    import gzip
    import shutil

    import requests

    from .main import logger
    from .zim import get_user_agent

    url = f"https://dumps.wikimedia.org/{edition}wiktionary/latest/{edition}wiktionary-latest-redirect.sql.gz"
    filename = url.rsplit("/", maxsplit=1)[-1]
    sql_gz_path = Path("build") / filename
    sql_path = sql_gz_path.with_name(sql_gz_path.stem)
    if not sql_path.exists() and not sql_gz_path.exists():
        logger.info(f"Downloading {filename}")
        r = requests.get(url, headers={"user-agent": get_user_agent()}, stream=True)
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
      from_id  INTEGER,
      title    TEXT,
      fragment TEXT,
      PRIMARY KEY (from_id, title)
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
    sql_path = download_redirect_sql_dump(edition)
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
            SELECT rd_from, rd_title, rd_fragment
            FROM redirect
            WHERE rd_namespace = 0 AND rd_interwiki = ''
            """)
            sqlite_conn.executemany(
                "INSERT INTO redirect VALUES(?, ?, ?)",
                (
                    (
                        from_id,
                        rd_title.decode("utf-8").replace("_", " "),
                        rd_fragment.decode("utf-8").replace(" ", "_"),
                    )
                    for from_id, rd_title, rd_fragment in cursor
                ),
            )

    sqlite_conn.execute("PRAGMA optimize")
    sqlite_conn.commit()
    sqlite_conn.close()
    zst_path = db_path.with_suffix(db_path.suffix + ".zst")
    with db_path.open("rb") as f_in, zstd.open(zst_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    db_path.unlink()
    logger.info("Redirect db created")
