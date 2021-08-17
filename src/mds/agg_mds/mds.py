import httpx
from mds import logger

# extend the default timeout
httpx.Client(timeout=20.0)


def pull_mds(baseURL: str, guid_type: str, batchSize: int = 1000) -> dict:
    """
    Pull all data from the MDS server at the baseURL. Will pull data using paging in set of "batchsize"
    until all data from NDS is completed. Note that the httpx get request probably needs a retry using
    tenacity or some other retry pattern.
    """

    more = True
    offset = 0
    results = {}
    while more:
        url = f"{baseURL}/mds/metadata?data=True&_guid_type={guid_type}&limit={batchSize}&offset={offset}"
        try:
            response = httpx.get(url)
            if response.status_code == 200:
                data = response.json()
                if len(data) < batchSize:
                    more = False
                else:
                    offset += batchSize
                results.update(data)
            else:
                more = False
                logger.error(f"An error occurred while requesting {url}.")
                raise ValueError(f"An error occurred while requesting {url}.")
        except Exception as exc:
            logger.error(f"An error occurred while requesting {exc}.")
            raise ValueError(f"An error occurred while requesting {url}.")

    return results
