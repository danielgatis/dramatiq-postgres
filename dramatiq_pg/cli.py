import argparse
import logging
import pdb
from pkg_resources import get_distribution

from dramatiq.cli import (
    LOGFORMAT,
    VERBOSITY,
)


logger = logging.getLogger(__name__)


def entrypoint():
    logging.basicConfig(level=logging.INFO, format=LOGFORMAT)

    try:
        exit(main())
    except (pdb.bdb.BdbQuit, KeyboardInterrupt):
        logger.info("Interrupted.")
    except Exception:
        logger.exception('Unhandled error:')
        logger.error(
            "Please file an issue at "
            "https://gitlab.com/dalibo/dramatiq-pg/issues/new with full log.",
        )
    exit(1)


def main():
    parser = make_argument_parser()
    args = parser.parse_args()

    logging.getLogger().setLevel(VERBOSITY.get(args.verbose, logging.INFO))

    return 0


def make_argument_parser():
    dist = get_distribution('dramatiq-pg')
    parser = argparse.ArgumentParser(
        prog="dramatiq-pg",
        description="Maintainance utility for task-queue in Postgres.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--version", action="version", version=dist.version)
    parser.add_argument(
        "--verbose", "-v", default=0, action="count",
        help="turn on verbose log output",
    )

    return parser


if '__main__' == __name__:
    entrypoint()
