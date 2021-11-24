from redis_wrapper.RedisHook import RedisHook

redis_connection = RedisHook(host='35.235.115.86', port=6379, password='TumbaPumba2093bX4KDxuRC6mALoLo')

if __name__ == "__main__":
  redis_connection.set_value(key=12, value="hello world")
  data = redis_connection.get_value(key=12)
  print(data)
