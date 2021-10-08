import mds.agg_mds.datastore.elasticsearch_dao as client

"""
This abstraction may seem pointless, but workds towards a few goals. This adds
a separation between the API and the data source, which helps mock any
datasource in tests. It also helps ensure the contract around the API without
it needing to know anything about the underlying persistence, and makes it
simpler to swap out and test new backends in the future.
"""


async def init(hostname, port):
    await client.init(hostname, port)


async def drop_all(commons_mapping):
    await client.drop_all(commons_mapping)


async def close():
    await client.close()


async def get_status():
    """
    Returns "OK" or raises an error indicating the status of the datastore:
    """
    return await client.get_status()


async def update_metadata(*args):
    await client.update_metadata(*args)


async def update_global_info(*args):
    await client.update_global_info(*args)


async def update_config_info(*args):
    await client.update_config_info(*args)


async def get_commons_metadata(*args):
    return await client.get_commons_metadata(*args)


async def get_all_tags():
    return await client.metadata_tags()


async def get_all_named_commons_metadata(*args):
    return await client.get_all_named_commons_metadata(*args)


async def get_by_guid(*args):
    return await client.get_by_guid(*args)


async def get_commons_attribute(*args):
    return await client.get_commons_attribute(*args)


async def get_commons():
    return await client.get_commons()


async def get_all_metadata(*args):
    return await client.get_all_metadata(*args)


async def get_aggregations(*args):
    return await client.get_aggregations(*args)


async def get_number_aggregations(*args):
    return await client.get_number_aggregation_for_field(*args)


async def search(*args):
    return await client.search(*args)
