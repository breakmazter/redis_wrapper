import pickle

from redis import ConnectionPool, StrictRedis


class RedisHook:
    def __init__(self, host, port, password, db=None):
        self.host = host
        self.port = port
        self.password = password
        self.db = db

    def get_connection(self):
        connection_pool = ConnectionPool(host=self.host,
                                         port=self.port,
                                         password=self.password,
                                         db=self.db)

        return StrictRedis(connection_pool=connection_pool)

    def key_exists(self, key: str) -> bool:
        return self.get_connection().exists(key)

    def set_value(self, key: str, value, ex=None, px=None, nx=False, xx=False):
        return self.get_connection().set(key, pickle.dumps(value), ex, px, nx, xx)

    def get_value(self, key: str):
        return pickle.loads(self.get_connection().get(key))

    def set_hset(self, name: str, key: str, value):
        return self.get_connection().set(name, key, pickle.dumps(value))

    def get_hset(self, name: str, key: str):
        return pickle.loads(self.get_connection().hget(name, key))

    def set_multy(self, keys: list, values):
        return self.get_connection().mset(dict(zip(keys, map(pickle.dumps, values))))

    def get_multy(self, keys: list):
        return list(map(pickle.loads, self.get_connection().mget(keys)))

    def delete_multy(self, keys):
        self.get_connection().delete(*keys)
