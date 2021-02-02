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
from jinja2 import Template


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
@click.option("--airflow-render-command", envvar="AIRFLOW_DAG_PROFILER__AIRFLOW_RENDER_COMMAND", default="airflow render {{dag_id}} {{bq_task}} {{ds}}")
@click.option("--airflow-list-tasks-command", envvar="AIRFLOW_DAG_PROFILER__AIRFLOW_LIST_TASKS_COMMAND", default="airflow list_tasks -t {{dag_id}} 2>/dev/null")
@click.option("--start-date", type=click.DateTime(), envvar="AIRFLOW_DAG_PROFILER__START_DATE")
@click.option("--end-date", type=click.DateTime(), envvar="AIRFLOW_DAG_PROFILER__END_DATE")
@click.option("--conversion-units", envvar="AIRFLOW_DAG_PROFILER__CONVERSION_UNITS")
def airflow_dag_profiler(dag_id, debug, date, airflow_list_tasks_command, airflow_render_command, start_date, end_date, conversion_units):
    if conversion_units is not None:
        m = re.match(r"^(?P<num>\d*(\.\d+)?)(?P<name>.+)$",conversion_units)
        assert m is not None and len(m.group("num")) > 0 and len(
            m.group("name")) > 0, f"bad conversion_units: \"{conversion_units}\""
        conversion_units = {"name":m.group("name"),"num":float(m.group("num"))}

    if debug:
        logging.basicConfig(level=logging.INFO)
    tasktree = _system(Template(airflow_list_tasks_command).render(
        {"dag_id": dag_id, "date": date})).output
    client = bigquery.Client()
    # <Task(BigQueryOperator): do_filter>
    bq_tasks = [t for t in re.findall(
        r"<Task\(BigQueryOperator\): ([a-zA-Z0-9_]+)>", tasktree)]
    bq_tasks = list(set(bq_tasks))
    quota = []
    for bq_task in tqdm(bq_tasks):
        sql = _system(
            Template(airflow_render_command).render({"dag_id": dag_id, "bq_task": bq_task, "date": date, "ds": date.strftime("%Y-%m-%d")})).output
        lines = sql.split("\n")
        start = next(i for i, line in enumerate(lines) if re.match(
            "^ *# property: sql *$", line) is not None)
        end = next(i for i, line in enumerate(lines) if re.match(
            "^ *# property: destination_dataset_table *$", line) is not None)
        sql = "\n".join(lines[start+2:end-1])
        total_bytes_processed = client.query(sql, job_config=bigquery.job.QueryJobConfig(
            dry_run=True, use_query_cache=False)).total_bytes_processed
        r = {"bq_task": bq_task, "total_bytes_processed": total_bytes_processed}
        if conversion_units is not None:
            r[conversion_units["name"]] = total_bytes_processed*conversion_units["num"]
        quota.append(r)
    df = pd.DataFrame(quota)
    print(df)
    print(Template("total: {{total_bytes_processed}} bytes{%if conversion_units%}={{total_bytes_processed*conversion_units.num}}{{conversion_units.name}}{%endif%}").render({
        "total_bytes_processed":sum(df.total_bytes_processed)*(1 if (start_date is None or end_date is None) else (end_date-start_date).days+1),
        "conversion_units":conversion_units
    }))


if __name__ == "__main__":
    airflow_dag_profiler()
