# To run workers:
#
#     dramatiq --verbose -p 2 -t 1 example
#
# To produce messages:
#
#     python example.py
#

import logging
import os
import pdb
import sys
from time import sleep

import dramatiq
import dramatiq_pg
import psycopg2.pool


logger = logging.getLogger(__name__)
# Empty connstring let's you configure psycogp2 using PG* env vars.
pool = psycopg2.pool.ThreadedConnectionPool(0, 4, "")
dramatiq.set_broker(dramatiq_pg.PostgresBroker(pool=pool))


@dramatiq.actor
def sleeper(param):
    sleep(param)


def main():
    for _ in range(10):
        sleeper.send(2)


if '__main__' == __name__:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)1.1s: %(message)s',
    )

    try:
        exit(main())
    except (pdb.bdb.BdbQuit, KeyboardInterrupt):
        logger.info("Interrupted.")
    except Exception:
        logger.exception('Unhandled error:')
        if sys.stdout.isatty():
            logger.debug("Dropping in debugger.")
            pdb.post_mortem(sys.exc_info()[2])

    exit(os.EX_SOFTWARE)
