#!/usr/bin/env python3
import importlib.metadata
import sys
from datetime import datetime
from argparse import ArgumentParser
from .file_system_snapshot import FileSystemSnapshot


def get_version_string():
    package_name = __name__.partition('.')[0]
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


def main():
    parser = ArgumentParser()

    parser.add_argument('--destination', '-o',
                        required=True,
                        help="""Path to store filesystem snapshots in. Reuse this destination in
                                the future to reuse the blob cache and prevent file duplication.""")

    parser.add_argument('--targets', '-i',
                        nargs='+',
                        help="Path(s) to recursively snapshot")

    parser.add_argument('--exclude', '-x',
                        nargs='*',
                        default=[],
                        help="""
                            Path patterns to omit from the snapshot. See
                            https://docs.python.org/3/library/pathlib.html#pathlib-pattern-language
                            for documentaiton on pattern syntax.
                        """ )

    parser.add_argument('--snapshot-name', '-n',
                        default=datetime.now().strftime('%F %H:%M'),
                        help=f"""Name to use for this snapshot's directory. Defaults to the current
                                 timestamp with filesystem-naming compatible delimiters.""")

    parser.add_argument('--hash-algorithm',
                        default='blake2b',
                        help="Which hashlib algorithm to compute file hashes. Default is blake2b.")

    parser.add_argument('--threads',
                        type=int,
                        default=4,
                        help="How many threads to use for processing files.")

    parser.add_argument('--garbage-collect-cache', '--gc',
                        dest='garbage_collect',
                        action="store_true",
                        help="Run garbage collection in the blob cache")

    parser.add_argument('--dry',
                        action="store_true",
                        help="""Only print what file operations would be performed instead of
                                actually performing them. Useful as a sanity check.""")

    parser.add_argument('--verbose', '-v',
                        action="store_true",
                        help="Output extra information during snapshot operations.")

    parser.add_argument('--version',
                        action="version",
                        version=f"%(prog)s {get_version_string()}",
                        help="Print version number and exit.")

    args = parser.parse_args()

    snapshot = FileSystemSnapshot(snapshots_dir    = args.destination,
                                  snapshot_name    = args.snapshot_name,
                                  target_paths     = args.targets,
                                  exclude_patterns = args.exclude,
                                  hash_algorithm   = args.hash_algorithm,
                                  threads          = args.threads,
                                  dry_mode         = args.dry,
                                  verbose          = args.verbose)
    snapshot.take_snapshot()

    if args.garbage_collect:
        snapshot.garbage_collect_blob_cache()

    return 0


if __name__ == '__main__':
    sys.exit(main())