import httpx
from mds import logger
import logging
from tenacity import (
    retry,
    RetryError,
    wait_random_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    before_sleep_log,
)

# extend the default timeout
httpx.Client(timeout=20.0)


@retry(
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(httpx.TimeoutException),
    wait=wait_random_exponential(multiplier=1, max=20),
    before_sleep=before_sleep_log(logger, logging.DEBUG),
)
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
            response.raise_for_status()

            data = response.json()
            if len(data) < batchSize:
                more = False
            else:
                offset += batchSize
            results.update(data)

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Aborting futher pulls"
            )
            break
    return results
