from pathlib import Path

from libzim.writer import Item


def get_user_agent() -> str:
    from importlib.metadata import version

    return f"snapshot/{version('snap')} (https://github.com/xxyzz/snapshot)"


def download_kiwix_zim(lang_3: str) -> Path:
    import subprocess
    import xml.etree.ElementTree as ET
    from datetime import datetime

    import requests

    # https://kiwix-tools.readthedocs.io/en/latest/kiwix-serve.html#new-opds-api
    r = requests.get(
        f"https://opds.library.kiwix.org/catalog/v2/entries?count=2&lang={lang_3}&category=wiktionary",
        headers={"user-agent": get_user_agent()},
    )
    root = ET.fromstring(r.text)
    url = ""
    file_date = None
    ns = {"": "http://www.w3.org/2005/Atom"}
    for entry in root.findall("entry", namespaces=ns):
        entry_date = datetime.fromisoformat(entry.find("updated", namespaces=ns).text)
        if file_date is None or entry_date > file_date:
            url = (
                entry.find("link[@type='application/x-zim']", namespaces=ns)
                .get("href")
                .removesuffix(".meta4")
            )
            file_date = entry_date

    zim_path = Path(f"build/{url.rsplit('/', 1)[-1]}")
    url += ".torrent"
    subprocess.run(["aria2c", "-d", "build", "--seed-time", "0", url], check=True)
    return zim_path


class MyItem(Item):
    def __init__(self, title, path, content, mimetype):
        super().__init__()
        self.path = path
        self.title = title
        self.content = content
        self.mimetype = mimetype

    def get_path(self):
        return self.path

    def get_title(self):
        return self.title

    def get_mimetype(self):
        return self.mimetype

    def get_contentprovider(self):
        from libzim.writer import StringProvider

        return StringProvider(self.content)

    def get_hints(self):
        from libzim.writer import Hint

        return {Hint.FRONT_ARTICLE: True}


def add_kiwix_pages(zim_creator, kiwix_zim_path: Path, ns_prefixes: tuple[str]):
    from libzim.reader import Archive
    from libzim.writer import Hint
    from lxml import etree, html

    kiwix_zim = Archive(kiwix_zim_path)
    for entry_id in range(kiwix_zim.all_entry_count):
        entry = kiwix_zim._get_entry_by_id(entry_id)
        if entry.title.startswith(ns_prefixes):
            if not entry.is_redirect:
                item = entry.get_item()
                doc = html.fromstring(bytes(item.content).decode("UTF-8"))
                xml_string = etree.tostring(doc, encoding="UTF-8", method="xml")
                zim_creator.add_item(
                    MyItem(item.title, item.path, xml_string, item.mimetype)
                )
            else:
                target_entry = entry.get_redirect_entry()
                zim_creator.add_redirection(
                    entry.path,
                    entry.title,
                    target_entry.path,
                    {Hint.FRONT_ARTICLE: True},
                )
        elif entry.path.startswith("_assets_"):
            item = entry.get_item()
            path = f"_assets_/{item.path.rsplit('/', 1)[-1]}"
            zim_creator.add_item(
                MyItem(item.title, path, bytes(item.content), item.mimetype)
            )


def add_parsoid_pages(zim_creator, suffixes: tuple[str]):
    import json
    import shutil
    from compression import zstd

    from .main import logger

    added_pages = set()
    for zst_path in Path("build").glob("*.zst"):
        logger.info(f"Adding pages from {zst_path.name} to zim")
        ndjson_path = zst_path.with_suffix(".ndjson")
        with zstd.open(zst_path, "rb") as f_in, ndjson_path.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        with ndjson_path.open() as f:
            for line in f:
                data = json.loads(line)
                if data["name"].endswith(suffixes) and data["name"] not in added_pages:
                    zim_creator.add_item(
                        MyItem(data["name"], data["name"], data["html"], "text/html")
                    )
                    added_pages.add(data["name"])
        ndjson_path.unlink()


def create_zim(edition: str):
    from datetime import UTC, datetime

    from libzim.writer import Compression, Creator

    from .edition import EDITIONS
    from .main import logger

    zim_path = Path(f"build/{edition}.zim")
    if zim_path.exists():
        zim_path.unlink()
    with (
        Creator(zim_path)
        .config_compression(Compression.zstd)
        .config_indexing(False, EDITIONS[edition]["lang"]) as creator
    ):
        # https://www.openzim.org/wiki/Metadata
        zim_name = f"wiktionary_{edition}"
        for name, value in {
            "Name": zim_name,
            "Title": zim_name,
            "Creator": "xxyzz",
            "Publisher": "xxyzz",
            "Date": datetime.now(UTC).strftime("%Y-%m-%d"),
            "Description": zim_name,
            "Language": EDITIONS[edition]["lang"],
        }.items():
            creator.add_metadata(name, value)
        creator.add_illustration(48, b"\x89PNG\x0d\x0a\x1a\x0a")
        if "kiwix" in EDITIONS[edition]:
            logger.info("Downloading zim")
            kiwix_path = download_kiwix_zim(EDITIONS[edition]["lang"])
            logger.info("Downloading zim done")
            add_kiwix_pages(creator, kiwix_path, EDITIONS[edition]["kiwix"])
            kiwix_path.unlink()
        else:
            add_parsoid_pages(creator, EDITIONS[edition]["main_ns_suffixes"])
