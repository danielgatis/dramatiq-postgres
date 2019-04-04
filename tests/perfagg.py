#
#       P E R F O R M A N C E   A G G R E G A T I O N
#
# This scripts accepts a list of .csv file as produced by perf.py, aggregate
# metrics and output a statistical report as a colon CSV.
#

import csv
import logging
import os
import pdb
import statistics
import sys

logger = logging.getLogger(__name__)


def main():
    csv_names = sys.argv[1:]
    lines = iter_lines(csv_names)
    logger.info("Rates unit is message per seconds.")
    print("role;proc;threads;mean_rate;median_rate;variance")
    for role, nproc, nthreads, s in aggregate_lines(lines):
        print(f"{role};{nproc};{nthreads};"
              f"{s['mean']:.1f};{s['median']:.1f};{s['variance']:.1f}")


def aggregate_lines(lines):
    stats = dict(
        sender=dict(),
        worker=dict(),
    )
    for line in lines:
        role, nproc, nthread, count, time = line
        serie = stats[role].setdefault((nproc, nthread), [])
        serie.append(int(count) / float(time))

    for role, rstats in sorted(stats.items()):
        for (nproc, nthread), rates in sorted(rstats.items()):
            mean = statistics.mean(rates)
            cstats = dict(
                mean=mean,
                median=statistics.median(rates),
                variance=statistics.pvariance(rates, mean),
            )
            yield role, nproc, nthread, cstats

    return stats


def iter_lines(csv_names):
    for name in csv_names:
        with open(name, 'r') as fo:
            yield from csv.reader(fo)


if '__main__' == __name__:
    debug = 'DEBUG' in os.environ
    logging.basicConfig(
        format='%(levelname)1.1s: %(message)s',
        level=logging.DEBUG if debug else logging.INFO,
    )

    try:
        exit(main())
    except (pdb.bdb.BdbQuit, KeyboardInterrupt):
        logger.exception("Interrupted.")
    except Exception:
        logger.exception('Unhandled error:')
        if debug and sys.stdout.isatty():
            logger.debug("Dropping in debugger.")
            pdb.post_mortem(sys.exc_info()[2])

    exit(os.EX_SOFTWARE)
