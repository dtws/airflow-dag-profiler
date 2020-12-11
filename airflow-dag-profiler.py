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
import re
from tqdm import tqdm
from google.cloud import bigquery
import pandas as pd


def _add_logger(f):
    logger = logging.getLogger(f.__name__)

    def _f(*args, **kwargs):
        return f(*args, logger=logger, **kwargs)
    _f.__name__ = f.__name__
    return _f


RetCodeAndOutput = namedtuple("RetCodeAndOutput", "retcode output")


@_add_logger
def _system(cmd, logger=None):
    """return (exitcode,output)"""
    logger.info(f"> {cmd}")
    exitcode, output = subprocess.getstatusoutput(cmd)
    return RetCodeAndOutput(retcode=exitcode, output=output)


@click.command()
@click.option("--debug/--no-debug", default=False)
@click.argument("dag_id")
@click.argument("date", type=click.DateTime())
def airflow_dag_profiler(dag_id, debug, date):
    if debug:
        logging.basicConfig(level=logging.INFO)
    tasktree = _system(f"airflow list_tasks -t {dag_id} 2>/dev/null").output
    client = bigquery.Client()
    # <Task(BigQueryOperator): do_filter>
    bq_tasks = [t for t in re.findall(
        r"<Task\(BigQueryOperator\): ([a-zA-Z0-9_]+)>", tasktree)]
    bq_tasks = list(set(bq_tasks))
    quota = []
    for bq_task in tqdm(bq_tasks):
        sql = _system(
            f"airflow render {dag_id} {bq_task} {date.strftime('%Y-%m-%d')}").output
        lines = sql.split("\n")
        start = next(i for i, line in enumerate(lines) if re.match(
            "^ *# property: sql *$", line) is not None)
        end = next(i for i, line in enumerate(lines) if re.match(
            "^ *# property: destination_dataset_table *$", line) is not None)
        sql = "\n".join(lines[start+2:end-1])
        total_bytes_processed = client.query(sql, job_config=bigquery.job.QueryJobConfig(
            dry_run=True, use_query_cache=False)).total_bytes_processed
        quota.append(
            {"bq_task": bq_task, "total_bytes_processed": total_bytes_processed})
    df = pd.DataFrame(quota)
    print(df)
    print(f"total: {sum(df.total_bytes_processed)} bytes")


if __name__ == "__main__":
    airflow_dag_profiler()
