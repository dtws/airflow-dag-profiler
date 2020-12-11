#!/usr/bin/env python3
"""===============================================================================

        FILE: airflow-dag-profiler.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (nailbiter@dtws-work.in)
ORGANIZATION: Datawise Inc.
     VERSION: ---
     CREATED: 2020-12-11T10:15:42.030633
    REVISION: ---

==============================================================================="""

import click
import logging
import os
import subprocess
from collections import namedtuple


def _add_logger(f):
    logger = logging.getLogger(f.__name__)

    def _f(*args, **kwargs):
        return f(*args, logger=logger, **kwargs)
    _f.__name__ = f.__name__
    return _f

RetCodeAndOutput = namedtuple("RetCodeAndOutput","retcode output")

@_add_logger
def _system(cmd, logger=None):
    """return (exitcode,output)"""
    logger.info(f"> {cmd}")
    exitcode,output = subprocess.getstatusoutput(cmd)
    return RetCodeAndOutput(retcode=exitcode,output=output)


@click.command()
@click.option("--debug/--no-debug", default=False)
@click.argument("dag_id")
def airflow_dag_profiler(dag_id, debug):
    if debug:
        logging.basicConfig(level=logging.INFO)
    tasktree = _system(f"airflow list_tasks -t {dag_id} 2>/dev/null").output
    print(f"tasktree: {tasktree}")


if __name__ == "__main__":
    airflow_dag_profiler()
