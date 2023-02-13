import pickle

from redis import (Redis,
                   StrictRedis,
                   ConnectionPool)


class RedisWrapper:
    def __init__(self, host: str, port: int, password: str, db: str = None) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.db = db

    def get_connection(self) -> Redis:
        return StrictRedis(
            connection_pool=ConnectionPool(host=self.host, port=self.port, password=self.password, db=self.db)
        )

    def key_exists(self, key: str) -> int:
        return self.get_connection().exists(key)

    def set_value(self, key: str, value):
        return self.get_connection().set(key, pickle.dumps(value))

    def get_value(self, key: str):
        return pickle.loads(self.get_connection().get(key))

    def set_multi(self, keys: list, values):
        return self.get_connection().mset(dict(zip(keys, map(pickle.dumps, values))))

    def get_multi(self, keys: list):
        return list(map(pickle.loads, self.get_connection().mget(keys)))

    def delete_multi(self, keys: list):
        self.get_connection().delete(*keys)
