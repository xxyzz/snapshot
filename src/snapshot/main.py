import logging

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def build(args):
    from .api import download_last_release, edition_has_update, get_access_token
    from .edition import EDITIONS
    from .parsoid import create_parsoid_files
    from .zim import create_zim

    access_token = get_access_token()
    if edition_has_update(args.edition, access_token):
        if "kiwix" in EDITIONS[args.edition]:
            create_zim(args.edition, access_token)
        create_parsoid_files(args.edition, 0, access_token)
    else:
        patterns = [f"{args.edition}wiktionary_namespace_0*"]
        if "kiwix" in EDITIONS[args.edition]:
            patterns.append(f"{args.edition}.zim")
        download_last_release(patterns)


def main():
    import argparse

    from .api import check_update
    from .edition import EDITIONS

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    check_parser = subparsers.add_parser("check")
    check_parser.set_defaults(func=check_update)
    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("edition", choices=EDITIONS.keys())
    build_parser.set_defaults(func=build)
    args = parser.parse_args()
    args.func(args)
