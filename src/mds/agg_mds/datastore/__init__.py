from .redis_cache import redis_cache as redis_client


async def init(hostname, port):
    await redis_client.init_cache(hostname, port)


async def drop_all():
    await redis_client.json_sets("commons", [])


async def close():
    await redis_client.close()


async def get_status():
    return await redis_client.get_status()


async def update_metadata(*args):
    await redis_client.update_metadata(*args)


async def get_commons_metadata(*args):
    return await redis_client.get_commons_metadata(*args)


async def get_all_named_commons_metadata(*args):
    return await redis_client.get_all_named_commons_metadata(*args)


async def get_commons_metadata_guid(*args):
    return await redis_client.get_commons_metadata_guid(*args)


async def get_commons_attribute(*args):
    return await redis_client.get_commons_attribute(*args)


async def get_commons():
    return await redis_client.get_commons()


async def get_all_metadata(*args):
    return await redis_client.get_all_metadata(*args)
