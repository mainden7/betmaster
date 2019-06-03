import sys
import psycopg2
import psycopg2.pool
import yaml
import argparse
import yaml.scanner
from threading import Thread
from betmaster.redis import RedisQueue
from betmaster.utils import check_positive, check_conf, get_configuration

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


class WorkersPool:
    """Initialize pool of workers that will listen redis queue for the event url then download and parse html page
    find arbitrage odds and save results into database"""

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
    """
    Worker
    MSG Example (raw and approx):
    - type: dict
    - format: pickle
    structure
    url: [url]
    metadata: [data]
    """

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
        # listen to queue
        while True:
            msg = self.conn.get('message')
            if msg == self.STOP_WORD:
                break
            try:
                # do some stuff here
                deserialized_msg = self._deserialize_msg(msg)
                self._process_message(deserialized_msg)
                pass
            except:
                pass

    def _deserialize_msg(self, msg):
        """"""
        return pickle.loads(msg)

    def _process_message(self, msg: dict):
        """"""


def main():
    """
    Workers pool initialization script
    """
    args = parse_args(sys.argv[1:])
    redis_conf = get_configuration(args['conf_path'], 'redis')
    redis_conn = RedisQueue(name=args.get('db_key'), **redis_conf)

    # configure at postgres config
    max_connections = 50
    if args['workers'] > max_connections:
        raise ValueError(f'To much workers use from 1 to {max_connections}')
    try:
        # database configuration
        db_conf = get_configuration(args['conf_path'], 'database')
        conn_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=max_connections,
            dsn=DSN.format(**db_conf['user']))
    except (FileNotFoundError,
            yaml.parser.ParserError,
            yaml.scanner.ScannerError,
            psycopg2.Error) as e:
        print(e)
        sys.exit()

    ths = []
    try:
        for i in range(args['workers']):
            worker = Worker(
                name=f'Worker #{i}',
                conn_pool=conn_pool)
            t = Thread(target=worker.run, )
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
    except KeyboardInterrupt:
        for _ in range(args['workers']):
            redis_conn.lput('message', 'STOP')
    finally:
        conn_pool.closeall()


if __name__ == '__main__':
    main()
