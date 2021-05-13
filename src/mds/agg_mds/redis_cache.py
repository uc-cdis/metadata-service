from typing import Optional, List, Dict, Set
import json
from typing import Any
from aioredis import Redis, create_redis_pool
from datetime import datetime


class RedisCache:
    def __init__(self):
        self.redis_cache: Optional[Redis] = None

    async def init_cache(self, hostname: str = "0.0.0.0", port: int = 6379):
        print(create_redis_pool)
        self.redis_cache = await create_redis_pool(
            f"redis://{hostname}:{port}/0?encoding=utf-8"
        )

    async def keys(self, pattern):
        return await self.redis_cache.keys(pattern)

    async def set(self, key, value):
        return await self.redis_cache.set(key, value)

    async def get(self, key):
        return await self.redis_cache.get(key)

    async def json_sets(self, key: str, value: Any, path: str = "."):
        return await self.redis_cache.execute("JSON.SET", key, path, json.dumps(value))

    async def json_get(self, key: str, path: str = "."):
        resp = await self.redis_cache.execute("JSON.GET", key, path)
        if not resp:
            return None
        return json.loads(resp)

    async def json_arr_appends(self, key: str, value: Any):
        await self.redis_cache.execute("JSON.ARRAPPEND", key, ".", json.dumps(value))

    async def json_arr_index(self, key: str, guid: str):
        await self.redis_cache.execute("JSON.ARRINDEX", key, ".guids", f'"{guid}"')

    async def close(self):
        self.redis_cache.close()
        await self.redis_cache.wait_closed()

    # Higher level functions

    async def update_metadata(
        self,
        name: str,
        data: dict,
        mapping: dict,
        guid_arr: List[str],
        tags: Dict[str, List[str]],
        info: Dict[str, str],
        aggregations: Dict[str, Dict[str, str]],
    ):
        await self.json_sets(f"{name}", {})
        await self.json_sets(name, data, ".metadata")
        await self.json_sets(name, mapping, ".field_mapping")
        await self.json_sets(name, guid_arr, ".guids")
        await self.json_sets(name, tags, ".tags")
        await self.json_sets(name, info, ".info")
        await self.json_sets(name, aggregations, ".aggregations")
        await self.set_status(name, len(data), "none")
        await self.json_arr_appends("commons", name)

    async def set_status(self, name: str, err: str, count: int):
        await self.json_sets(
            f"{name}.status",
            {
                "last_update": datetime.now().strftime("%Y%m%d%H%M%S"),
                "error": err,
                "count": count,
            },
        )

    async def get_status(self):
        commons = await self.json_get("commons")
        results = {}
        for name in commons:
            results.update({name: await self.json_get(f"{name}.status")})
        return results

    async def get_commons_metadata(self, name: str, limit: int, offset: int):
        resp = await self.json_get(name, ".metadata")
        if resp is None:
            return None
        return resp[offset : offset + limit]

    async def get_all_named_commons_metadata(self, name: str):
        return await self.json_get(name, ".metadata")

    async def get_commons_metadata_guid(self, name: str, guid: str):
        resp = await self.json_get(name, f".metadata")
        if resp is None:
            return None
        idx = await self.json_arr_index(name, guid)
        if idx is None:
            return None
        return resp[idx]

    async def get_commons_attribute(self, name: str, what: str):
        return await self.json_get(name, what)

    async def get_commons(self):
        resp = await self.json_get("commons")
        if resp is None:
            return None
        return {"commons": resp}

    async def get_all_metadata(self, limit: int, offset: int):
        commons = await self.get_commons()
        results = {}
        if commons is None:
            return {}
        for name in commons["commons"]:
            results[name] = await self.get_commons_metadata(name, limit, offset)
        return results


redis_cache = RedisCache()
