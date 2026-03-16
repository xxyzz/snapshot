from pathlib import Path

from libzim.writer import Item


def download_kiwix_zim(lang_3: str) -> Path:
    import xml.etree.ElementTree as ET

    import requests

    r = requests.get(
        f"https://browse.library.kiwix.org/catalog/v2/entries?count=-1&lang={lang_3}&category=wiktionary"
    )
    root = ET.fromstring(r.text)
    url = (
        root.find(
            "entry/link[@type='application/x-zim']",
            namespaces={"": "http://www.w3.org/2005/Atom"},
        )
        .get("href")
        .removesuffix(".meta4")
    )
    r = requests.get(url, stream=True)
    zim_path = Path(f"build/{url.rsplit('/', 1)[-1]}")
    if not zim_path.exists():
        with zim_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
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

    kiwix_zim = Archive(kiwix_zim_path)
    for entry_id in range(kiwix_zim.all_entry_count):
        entry = kiwix_zim._get_entry_by_id(entry_id)
        if entry.title.startswith(ns_prefixes):
            if not entry.is_redirect:
                item = entry.get_item()
                zim_creator.add_item(
                    MyItem(item.title, item.path, bytes(item.content), item.mimetype)
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


def create_zim(edition: str, access_token: str):
    from datetime import UTC, datetime
    from importlib.resources import files

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
        with open(files("snapshot") / "wiktionary.png", "rb") as f:
            creator.add_illustration(48, f.read())
        logger.info("Downloading zim")
        kiwix_path = download_kiwix_zim(EDITIONS[edition]["lang"])
        logger.info("Downloading zim done")
        add_kiwix_pages(creator, kiwix_path, EDITIONS[edition]["kiwix"])
        kiwix_path.unlink()
