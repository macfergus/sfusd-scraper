import argparse
import json

import MySQLdb


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

    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS tasks;')
    c.execute('DROP TABLE IF EXISTS subtasks;')
    c.execute('''
        CREATE TABLE tasks (
            pk SERIAL PRIMARY KEY NOT NULL,
            snapshot_ts BIGINT NOT NULL,
            task_id MEDIUMINT NOT NULL,
            task_desc MEDIUMTEXT,
            progress DECIMAL(5,3) NOT NULL,
            INDEX(snapshot_ts),
            INDEX(task_id)
        ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    ''')
    c.execute('''
        CREATE TABLE subtasks (
            pk SERIAL PRIMARY KEY NOT NULL,
            snapshot_ts BIGINT NOT NULL,
            task_id MEDIUMINT NOT NULL,
            subtask_id MEDIUMINT NOT NULL,
            subtask_desc MEDIUMTEXT,
            progress DECIMAL(5,3) NOT NULL,
            INDEX(snapshot_ts),
            INDEX(task_id),
            INDEX(subtask_id)
        ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    ''')


if __name__ == '__main__':
    main()
