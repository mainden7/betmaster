import redis


class RedisQueue:
    def __init__(self, _namespace='bet', **redis_kwargs):
        self.__db = redis.Redis(**redis_kwargs)
        self.__db.ping()
        self._namespace = _namespace

    def qsize(self, key):
        """Return the approximate size of the queue."""

        return self.__db.llen(f'{self._namespace}:{key}')

    def empty(self, key):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize(key) == 0

    def increment(self, key):
        self.__db.incr(f'{self._namespace}:{key}')

    def decrement(self, key):
        self.__db.decr(f'{self._namespace}:{key}')

    def set(self, key, value):
        self.__db.set(f'{self._namespace}:{key}', value)

    def get_val(self, key):
        return self.__db.get(f'{self._namespace}:{key}')

    def delete(self, key):
        self.__db.delete(f'{self._namespace}:{key}')

    def put(self, key, item):
        """Put item into the queue."""
        self.__db.rpush(f'{self._namespace}:{key}', item.encode())

    def lput(self, key, item):
        self.__db.lpush(f'{self._namespace}:{key}', item.encode())

    def get(self, key, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(f'{self._namespace}:{key}', timeout=timeout)
        else:
            item = self.__db.lpop(f'{self._namespace}:{key}')

        if item:
            item = item[1]
        return item.decode('utf-8')

    def rget(self, key, block=True, timeout=None):
        """Remove and return an item from the queue.

                If optional args block is true and timeout is None (the default), block
                if necessary until an item is available."""
        if block:
            item = self.__db.brpop(f'{self._namespace}:{key}', timeout=timeout)
        else:
            item = self.__db.rpop(f'{self._namespace}:{key}')

        if item:
            item = item[1]
        return item.decode('utf-8')

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

    def clear(self):
        self.__db.flushall()
