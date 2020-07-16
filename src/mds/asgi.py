from .main import get_app
from . import logger

app = get_app()


@app.on_event("shutdown")
def shutdown_event():
    logger.info("Closing async client.")
    global app
    app.async_client.close()
