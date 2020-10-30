"""
Usage:
- Run app: python run.py
- Generate openapi docs: python run.py openapi
"""

import os
import sys
import uvicorn
import yaml
import cdislogging

from mds.main import get_app


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

cdislogging.get_logger(None, log_level="warn")
for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
    cdislogging.get_logger(logger_name, log_level="info")

if __name__ == "__main__":
    if sys.argv[-1] == "openapi":
        schema = get_app().openapi()
        path = os.path.join(CURRENT_DIR, "docs/openapi.yaml")
        yaml.Dumper.ignore_aliases = lambda *args: True
        with open(path, "w+") as f:
            yaml.dump(schema, f, default_flow_style=False)
        print(f"Saved docs at {path}")
    else:
        uvicorn.run("mds.asgi:app", reload=True, log_config=None)
