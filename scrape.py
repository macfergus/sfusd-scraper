import argparse
import json
import os
import shutil
import tempfile
import time
import zipfile
from collections import namedtuple
from decimal import Decimal

import MySQLdb
import requests
from tableauhyperapi import HyperProcess, Connection, Telemetry


Task = namedtuple('Task', 'task_id task_desc progress')
Subtask = namedtuple('Subtask', 'task_id subtask_id subtask_desc progress')


def get_twbx_file(output_dir):
    dashboard_url = 'https://public.tableau.com/workbooks/SFUSDReopeningReadinessDashboard.twb'
    outputfile = os.path.join(output_dir, 'dashboard.twbx')
    r = requests.get(dashboard_url)
    with open(outputfile, 'wb') as outf:
        outf.write(r.content)
    return outputfile


def get_dbs(twbx_file, output_dir):
    with zipfile.ZipFile(twbx_file, 'r') as z:
        for fname in z.namelist():
            if fname.endswith('.hyper'):
                z.extract(fname, output_dir)
                yield os.path.join(output_dir, fname)


def to_decimal(f):
    return Decimal(int(1000 * f)) / Decimal(1000)


def has_tasks_table(hyper_conn):
    task_table = None
    tables = hyper_conn.catalog.get_table_names('Extract')
    for t in tables:
        if t.name.unescaped.startswith('SFUSD Tasks'):
            return True
    return False


def get_tasks(hyper_conn):
    task_table = None
    tables = hyper_conn.catalog.get_table_names('Extract')
    for t in tables:
        if t.name.unescaped.startswith('SFUSD Tasks'):
            task_table = t
    if task_table is None:
        raise ValueError('No task table')

    results = []
    with hyper_conn.execute_query(f'SELECT * FROM {task_table}') as result:
        for row in result:
            task_id, task_desc, _, _, _, progress = row[:6]
            if task_desc is None:
                # Seems to be some dummy rows in here
                continue
            results.append(Task(
                task_id=int(task_id),
                task_desc=task_desc,
                progress=to_decimal(progress)
            ))
    return results


def get_subtasks(hyper_conn):
    subtask_table = None
    tables = hyper_conn.catalog.get_table_names('Extract')
    for t in tables:
        if t.name.unescaped.startswith('SFUSD Subtasks'):
            subtask_table = t
    if subtask_table is None:
        raise ValueError('No subtask table')

    results = []
    with hyper_conn.execute_query(f'SELECT * FROM {subtask_table}') as result:
        for row in result:
            task_id, subtask_id, subtask_desc, _, progress = row[:5]
            results.append(Subtask(
                task_id=int(task_id),
                subtask_id=int(subtask_id),
                subtask_desc=subtask_desc,
                progress=to_decimal(progress or 0.0)
            ))
    return results


def save_tasks(conn, snapshot_ts, tasks, subtasks):
    c = conn.cursor()
    for t in tasks:
        c.execute('''
            INSERT INTO tasks (snapshot_ts, task_id, task_desc, progress)
            VALUES (%s, %s, %s, %s)
        ''', (snapshot_ts, t.task_id, t.task_desc, t.progress))
    for s in subtasks:
        c.execute('''
            INSERT INTO subtasks
                (snapshot_ts, task_id, subtask_id, subtask_desc, progress)
            VALUES (%s, %s, %s, %s, %s)
        ''', (snapshot_ts, s.task_id, s.subtask_id, s.subtask_desc, s.progress))
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config', '-c', required=True,
        help='Config file with database connection info'
    )
    args = parser.parse_args()

    config = json.load(open(args.config))

    conn = MySQLdb.connect(
        user=config['db_user'],
        passwd=config['db_passwd'],
        db=config['db_name']
    )

    with tempfile.TemporaryDirectory(prefix='sfusd-scraper') as tmpdir:
        snapshot_ts = int(time.time())
        twbx_file = get_twbx_file(tmpdir)
        for db_file in get_dbs(twbx_file, tmpdir):
            with HyperProcess(
                    Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'sfusd-scraper'
            ) as hyper:
                with Connection(hyper.endpoint, db_file) as connection:
                    if has_tasks_table(connection):
                        tasks = get_tasks(connection)
                        subtasks = get_subtasks(connection)
                        break

        save_tasks(conn, snapshot_ts, tasks, subtasks)


if __name__ == '__main__':
    main()
