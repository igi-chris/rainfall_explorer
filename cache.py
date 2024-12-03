import cachetools

# Create a cache with a maximum size
local_cache = cachetools.TTLCache(maxsize=1000, ttl=60*60*24)  # 24 hour TTL

def get_local_cache():
    return local_cache
