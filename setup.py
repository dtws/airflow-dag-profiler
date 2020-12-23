"""===============================================================================

        FILE: setup.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (nailbiter@dtws-work.in)
ORGANIZATION: Datawise Inc.
     VERSION: ---
     CREATED: 2020-12-22T18:44:25.406752
    REVISION: ---

==============================================================================="""

from setuptools import setup, find_packages


def get_install_requires(fn="requirements.txt"):
    with open(fn) as f:
        lines = f.readlines()
    lines = map(lambda s: s.strip(), lines)
    lines = filter(lambda s: not s.startswith("#"), lines)
    return list(lines)

with open(".version.txt") as f:
    version = f.read().strip()

setup(
    name='airflow_dag_profiler',
    version=version,
    py_modules=["airflow_dag_profiler"],
    install_requires=get_install_requires(),
    entry_points='''
        [console_scripts]
        airflow-dag-profiler=airflow_dag_profiler:airflow_dag_profiler
    ''',
)
