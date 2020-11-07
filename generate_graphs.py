import argparse
import io
import json
import os
import time

import MySQLdb
import numpy as np
import pandas as pd
import plotly.express as px
import pytz


HEADER = '''
<html>
<head>
<style>
body {
    font-family: sans-serif
}
</style>
</head>
<body>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<div style="explanation">
<p>These graphs are <strong>unofficial</strong>! The data was scraped from the
<a href="https://public.tableau.com/profile/thu.cung#!/vizhome/SFUSDReopeningReadinessDashboard/2ADashboardandProgressView?publish=yes">Official SFUSD dashboard</a>. I try my best to keep it up to date, but I can't guarantee the accuracy of this page.
</p>
<p>Source code <a href="https://github.com/macfergus/sfusd-scraper">here</a></p>
</div>
'''

FOOTER = '''
</body>
</html>
'''


def truncate(s):
    maxlen = 50
    if len(s) <= maxlen:
        return s
    return s[:maxlen] + '...'


def load_tasks(conn):
    df = []
    c = conn.cursor()
    c.execute('''
        SELECT snapshot_ts, task_id, task_desc, progress, phase
        FROM tasks
        ORDER BY snapshot_ts ASC
    ''')
    for row in c.fetchall():
        snapshot_ts, task_id, task_desc, progress, phase = row
        df.append({
            'snapshot_ts': snapshot_ts,
            'task_id': task_id,
            'task_desc': task_desc,
            'progress': progress,
            'phase': phase,
        })
    df = pd.DataFrame(df)
    df['utc_date'] = pd.to_datetime(df.snapshot_ts, unit='s')
    df['date'] = (
        df.utc_date.dt.tz_localize('UTC')
        .dt.tz_convert('America/Los_Angeles')
    )
    df['desc_short'] = df['task_desc'].map(truncate)
    df['progress'] = 100.0 * df.progress.map(float)
    return df


def load_subtasks(conn):
    df = []
    c = conn.cursor()
    c.execute('''
        SELECT snapshot_ts, task_id, subtask_id, subtask_desc, progress, phase
        FROM subtasks
        ORDER BY snapshot_ts ASC
    ''')
    for row in c.fetchall():
        snapshot_ts, task_id, subtask_id, subtask_desc, progress, phase = row
        df.append({
            'snapshot_ts': snapshot_ts,
            'task_id': task_id,
            'subtask_id': subtask_id,
            'subtask_desc': subtask_desc,
            'progress': progress,
            'phase': phase,
        })
    df = pd.DataFrame(df)
    df['utc_date'] = pd.to_datetime(df.snapshot_ts, unit='s')
    df['date'] = (
        df.utc_date.dt.tz_localize('UTC')
        .dt.tz_convert('America/Los_Angeles')
    )
    df['desc_short'] = df.subtask_desc.map(truncate)
    df['progress'] = 100.0 * df.progress.map(float)
    return df


def generate_task_graph(df):
    fig = px.line(
        df,
        x='date', y='progress',
        color='desc_short',
        labels={
            'date': 'Date',
            'progress': 'Progress',
            'desc_short': 'Task',
        },
        title='Overall'
    )
    buf = io.StringIO()
    fig.write_html(buf, include_plotlyjs=False, full_html=False)
    return buf.getvalue()


def generate_subtask_graph(df, task):
    fig = px.line(
        df,
        x='date', y='progress',
        color='desc_short',
        labels={
            'date': 'Date',
            'progress': 'Progress',
            'desc_short': 'Subtask',
        },
        title=task
    )
    buf = io.StringIO()
    fig.write_html(buf, include_plotlyjs=False, full_html=False)
    return buf.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config', '-c', required=True,
        help='Config file with database connection info'
    )
    parser.add_argument(
        '--output', '-o', required=True,
        help='Output file'
    )
    args = parser.parse_args()

    config = json.load(open(args.config))

    conn = MySQLdb.connect(
        user=config['db_user'],
        passwd=config['db_passwd'],
        db=config['db_name']
    )

    task_df = load_tasks(conn)
    subtask_df = load_subtasks(conn)
    last_update = task_df.date.max()

    for phase in ['2A', '2B']:
        task_subset = task_df.query('phase == @phase')
        subtask_subset = subtask_df.query('phase == @phase')
        graphs = []
        graphs.append(generate_task_graph(task_subset))
        for task_id, subset in subtask_subset.groupby('task_id'):
            task_desc = task_df.query('task_id == @task_id').iloc[0].task_desc
            graphs.append(generate_subtask_graph(subset, task_desc))

        if phase == '2A':
            outfile = os.path.join(args.output, 'index.html')
        else:
            outfile = os.path.join(args.output, '2b.html')

        with open(outfile, 'w') as outf:
            outf.write(HEADER + "\n")
            for div in graphs:
                outf.write(div)
            outf.write(f'''
                <div class="footer">
                Last updated {last_update}
                </div>
            ''')
            outf.write(FOOTER)


if __name__ == '__main__':
    main()
