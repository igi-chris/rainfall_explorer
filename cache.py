import os

import redis
import fakeredis

# use fake redis if running locally and you don't need to keep the cache
# between runs
USE_FAKE_REDIS = os.getenv('USE_FAKE_REDIS', '').lower() in ('true', 't', '1', 'yes', 'y')

def get_redis_client() -> redis.Redis:
    """
    Returns a Redis client. Uses fakeredis if running in a test environment
    or if USE_FAKE_REDIS is set to True.
    """
    # Use fakeredis in test environments or if USE_FAKE_REDIS is True
    if USE_FAKE_REDIS or 'TEST_RUN_PIPE' in os.environ or 'UNIT_TESTING' in os.environ:
        redis_client: redis.Redis = fakeredis.FakeRedis()
        print("Using FakeRedis for local storage.")
    else:
        host = os.getenv("IGI_ML_REDIS_URL", "localhost")
        port = int(os.getenv("IGI_ML_REDIS_PORT", "6379"))
        redis_pwd = os.getenv("IGI_ML_REDIS_PWD", None)
        ssl = os.getenv("IGI_ML_REDIS_SSL", "").lower() in ('true', 't', '1', 'yes', 'y')
        db = int(os.getenv("IGI_ML_REDIS_DB", "9"))

        try:
            redis_client = redis.Redis(
                host=host, port=port, password=redis_pwd, ssl=ssl, db=db
            )
            # Test the connection
            redis_client.ping()
            print(f"Connected to Redis db {db} at {host}:{port}.")
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            print(f"Failed to connect to Redis: {e}. Using FakeRedis instead.")
            redis_client = fakeredis.FakeRedis()

    return redis_client
