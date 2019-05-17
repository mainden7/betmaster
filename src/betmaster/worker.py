import os
import sys
from threading import Thread

import yaml
import argparse
import yaml.scanner
from betmaster.redis import RedisQueue

NAMESPACE = 'betmaster'
DSN = 'dbname={database} user={user} password={password} host={host}'


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', '-w',
                        help='Quantity of workers to be launched in current session',
                        type=check_positive,
                        default=1)
    parser.add_argument('--conf-path',
                        help='Path to the db configuration',
                        type=check_conf,
                        required=True)

    return vars(parser.parse_args(args))


def check_positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError(
            f"{value} is an invalid positive int value")
    return value


def check_conf(path):
    actions_conf = os.path.abspath(path)
    try:
        with open(actions_conf, 'r') as f:
            yaml.load(f.read())
    except (yaml.parser.ParserError,
            yaml.scanner.ScannerError) as e:
        raise argparse.ArgumentTypeError(f'Invalid configuration file {e}')
    except FileNotFoundError:
        raise argparse.ArgumentTypeError(f'No such file: {actions_conf}')
    return actions_conf


def get_configuration(conf_path, service):
    with open(conf_path, 'r') as f:
        db_conf = yaml.load(f.read())
    return db_conf[service]


class WorkersPool:
    def __init__(self, count, conn_pool):
        self.count = count
        self.conn_pool = conn_pool

        self.workers = []
        self.threads = []

    def start_pool(self):
        for i in range(self.count):
            worker = Worker(
                name='Worker #{}'.format(i),
                conn_pool=self.conn_pool)
            self.workers.append(worker)

            t = Thread(target=worker.run)
            t.start()
            self.threads.append(t)

    def close_pool(self):
        for w in self.workers:
            w.stop(force=False)

        for t in self.threads:
            t.join()


class Worker:
    STOP_WORD = "STOP"

    def __init__(self, name, conn_pool):
        self.name = name
        self.conn = RedisQueue(name=NAMESPACE, host='0.0.0.0')
        self.db_conn = conn_pool.getconn()

    def stop(self, force=False):
        if force:
            self.conn.lput('message', 'STOP')
        else:
            self.conn.put('message', 'STOP')

    def run(self):
        while True:
            msg = self.conn.get('message')
            if msg == self.STOP_WORD:
                break
            try:
                # do some stuff here
                pass
            except:
                pass


def main():
    args = parse_args(sys.argv[1:])
    redis_conf = get_configuration(args['conf_path'], 'redis')
    stop_conn = RedisQueue(name=args.get('db_key'), **redis_conf)

    max_connections = 50
    if args['workers'] > max_connections:
        raise ValueError('To much workers use from 1 to {}'.format(max_connections))
    try:
        # database configuration
        db_conf = get_configuration(args['conf_path'], 'database')
        conn_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=max_connections,
            dsn=DSN.format(**db_conf['user']))
    except (FileNotFoundError, yaml.parser.ParserError, yaml.scanner.ScannerError, psycopg2.Error) as e:
        print(e)
        sys.exit()

    ths = []
    try:
        for i in range(args['workers']):
            worker = Worker(name='Worker #{}'.format(i), network=args.get('db_key'), conn_pool=conn_pool)
            worker.register()
            t = Thread(target=worker.run, )
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
    except KeyboardInterrupt:
        for _ in range(args['workers']):
            stop_conn.lput('message', 'STOP')
    finally:
        conn_pool.closeall()


if __name__ == '__main__':
    main()
